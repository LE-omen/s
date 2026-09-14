//! Throwaway V10 process transport. Bounded framed child pipes prove the real
//! SQL/storage split first. This synchronous adapter is not the planned Mio IPC
//! reactor and does not establish a high-concurrency performance claim.
use std::ffi::c_void;
use std::io::{self, Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_FRAME: usize = 256 * 1024;

fn read_frame(input: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut header = [0; 8];
    input.read_exact(&mut header)?;
    let len = u32::from_le_bytes(header[4..].try_into().unwrap()) as usize;
    if header[..4] != *b"NS10" || len == 0 || len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid worker frame",
        ));
    }
    let mut payload = vec![0; len];
    input.read_exact(&mut payload)?;
    Ok(payload)
}

fn write_frame(output: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    if payload.is_empty() || payload.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid worker frame size",
        ));
    }
    let mut header = [0; 8];
    header[..4].copy_from_slice(b"NS10");
    header[4..].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    output.write_all(&header)?;
    output.write_all(payload)?;
    output.flush()
}

struct Worker {
    child: Child,
    input: ChildStdin,
    replies: Option<Receiver<io::Result<Vec<u8>>>>,
    reader: Option<JoinHandle<()>>,
    last: Vec<u8>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.replies.take();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn spawn(namespace: u64, generation: u64) -> io::Result<Worker> {
    let base = std::env::current_dir()?
        .join("run")
        .join(format!("namespace-worker-{namespace}-{generation}"));
    std::fs::create_dir_all(&base)?;
    let stderr = std::fs::File::create(base.join("process.out"))?;
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("--namespace-sql-worker-prototype")
        .arg(format!("@{namespace}"))
        .current_dir(base)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(stderr);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let max_fd = unsafe { libc::sysconf(libc::_SC_OPEN_MAX) };
        if max_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // Engine C++ files are not all CLOEXEC. Touch only the child's copy:
        // stdio survives, all other descriptors close on successful exec. Keep
        // the spawn error pipe open until exec so launch failures still report.
        // The pre_exec closure performs only async-signal-safe fcntl operations.
        // ponytail: bounded POSIX fd sweep; optimize spawn after measuring it.
        unsafe {
            command.pre_exec(move || {
                for fd in 3..max_fd {
                    loop {
                        if libc::fcntl(fd as libc::c_int, libc::F_SETFD, libc::FD_CLOEXEC) != -1 {
                            break;
                        }
                        let error = io::Error::last_os_error();
                        match error.raw_os_error() {
                            Some(libc::EBADF) => break,
                            Some(libc::EINTR) => continue,
                            _ => return Err(error),
                        }
                    }
                }
                Ok(())
            });
        }
    }
    let mut child = command.spawn()?;
    let input = child.stdin.take().unwrap();
    let mut output = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::sync_channel(1);
    let reader = match thread::Builder::new()
        .name("ns-proto-read".into())
        .spawn(move || loop {
            let frame = read_frame(&mut output);
            let failed = frame.is_err();
            if tx.send(frame).is_err() || failed {
                break;
            }
        }) {
        Ok(reader) => reader,
        Err(err) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(err);
        }
    };
    Ok(Worker {
        child,
        input,
        replies: Some(rx),
        reader: Some(reader),
        last: Vec::new(),
    })
}

// Opaque pointers below are process-local C ABI handles, never wire fields.
#[no_mangle]
pub unsafe extern "C" fn namespace_proto_spawn(
    namespace: u64,
    generation: u64,
    pid: *mut u32,
) -> *mut c_void {
    if pid.is_null() {
        return std::ptr::null_mut();
    }
    match spawn(namespace, generation) {
        Ok(worker) => {
            *pid = worker.child.id();
            Box::into_raw(Box::new(worker)).cast()
        }
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn namespace_proto_send(
    worker: *mut c_void,
    data: *const u8,
    len: usize,
) -> i32 {
    if worker.is_null() || data.is_null() || len == 0 || len > MAX_FRAME {
        return -1;
    }
    let worker = &mut *worker.cast::<Worker>();
    match write_frame(&mut worker.input, std::slice::from_raw_parts(data, len)) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn namespace_proto_receive(
    worker: *mut c_void,
    data: *mut *const u8,
    len: *mut usize,
    timeout_ms: u64,
) -> i32 {
    if worker.is_null() || data.is_null() || len.is_null() {
        return -1;
    }
    let worker = &mut *worker.cast::<Worker>();
    match worker
        .replies
        .as_ref()
        .unwrap()
        .recv_timeout(Duration::from_millis(timeout_ms))
    {
        Ok(Ok(frame)) => {
            worker.last = frame;
            *data = worker.last.as_ptr();
            *len = worker.last.len();
            0
        }
        _ => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn namespace_proto_stop(worker: *mut c_void) {
    if !worker.is_null() {
        drop(Box::from_raw(worker.cast::<Worker>()));
    }
}

#[no_mangle]
pub unsafe extern "C" fn namespace_proto_worker_read(
    data: *mut u8,
    capacity: usize,
    len: *mut usize,
) -> i32 {
    if data.is_null() || len.is_null() {
        return -1;
    }
    match read_frame(&mut io::stdin().lock()) {
        Ok(frame) if frame.len() <= capacity => {
            std::ptr::copy_nonoverlapping(frame.as_ptr(), data, frame.len());
            *len = frame.len();
            0
        }
        _ => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn namespace_proto_worker_write(data: *const u8, len: usize) -> i32 {
    if data.is_null() || len == 0 || len > MAX_FRAME {
        return -1;
    }
    match write_frame(
        &mut io::stdout().lock(),
        std::slice::from_raw_parts(data, len),
    ) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

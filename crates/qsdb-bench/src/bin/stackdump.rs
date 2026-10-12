//! 进程外全线程栈转储器（挂死诊断）：stackdump.exe <pid>。
//! SuspendThread + StackWalk64 + SymFromAddr；目标须带 line-tables。
#[cfg(windows)]
mod imp {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Diagnostics::Debug::{
        StackWalk64, SymFromAddr, SymCleanup, SymInitialize, CONTEXT, STACKFRAME64, SYMBOL_INFO,
    };
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
    };
    use windows_sys::Win32::System::SystemInformation::IMAGE_FILE_MACHINE_AMD64;
    use windows_sys::Win32::System::Threading::{
        GetCurrentThreadId, OpenProcess, OpenThread, ResumeThread, SuspendThread,
        THREAD_GET_CONTEXT, THREAD_QUERY_INFORMATION, THREAD_SUSPEND_RESUME,
    };
    use windows_sys::Win32::System::Diagnostics::Debug::GetThreadContext;

    pub fn dump(pid: u32) {
        unsafe {
            let snap = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
            let mut me: THREADENTRY32 = std::mem::zeroed();
            me.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
            let mut threads = Vec::new();
            if Thread32First(snap, &mut me) != 0 {
                loop {
                    if me.th32OwnerProcessID == pid {
                        threads.push(me.th32ThreadID);
                    }
                    if Thread32Next(snap, &mut me) == 0 {
                        break;
                    }
                }
            }
            windows_sys::Win32::Foundation::CloseHandle(snap);
            eprintln!("=== pid {pid} threads: {} ===", threads.len());

            let hproc = OpenProcess(0x1FFFFF, 0, pid);
            SymInitialize(hproc, std::ptr::null(), 1);
            let cur = GetCurrentThreadId();

            for tid in threads {
                let h = OpenThread(
                    THREAD_SUSPEND_RESUME | THREAD_GET_CONTEXT | THREAD_QUERY_INFORMATION,
                    0,
                    tid,
                );
                if h.is_null() {
                    continue;
                }
                if SuspendThread(h) == u32::MAX {
                    CloseHandle(h);
                    continue;
                }
                let mut frames: Vec<u64> = Vec::new();
                let mut ctx: CONTEXT = std::mem::zeroed();
                ctx.ContextFlags = 0x10003F; // CONTEXT_FULL
                if GetThreadContext(h, &mut ctx) != 0 {
                    let mut sf: STACKFRAME64 = std::mem::zeroed();
                    sf.AddrPC.Offset = ctx.Rip as u64;
                    sf.AddrPC.Mode = 0;
                    sf.AddrStack.Offset = ctx.Rsp as u64;
                    sf.AddrStack.Mode = 0;
                    sf.AddrFrame.Offset = ctx.Rbp as u64;
                    sf.AddrFrame.Mode = 0;
                    for _ in 0..48 {
                        frames.push(sf.AddrPC.Offset);
                        let ok = StackWalk64(
                            IMAGE_FILE_MACHINE_AMD64 as u32,
                            hproc,
                            h,
                            &mut sf,
                            &mut ctx as *mut CONTEXT as *mut core::ffi::c_void,
                            None,
                            None,
                            None,
                            None,
                        );
                        if ok == 0 || sf.AddrPC.Offset == 0 {
                            break;
                        }
                    }
                }
                ResumeThread(h);
                eprintln!("--- thread {tid} {} ---", if tid == cur { "(MAIN)" } else { "" });
                let mut buf = vec![0u8; 4096];
                for (i, addr) in frames.iter().enumerate() {
                    if *addr == 0 {
                        continue;
                    }
                    let sym = buf.as_mut_ptr() as *mut SYMBOL_INFO;
                    (*sym).SizeOfStruct = 88; // Name 数组前的字节数
                    (*sym).MaxNameLen = 4000;
                    let mut disp = 0u64;
                    let name = if SymFromAddr(hproc, *addr, &mut disp, sym) != 0 {
                        let p = buf.as_ptr().add(88);
                        let mut s = String::new();
                        let mut q = p;
                        let mut guard = 0;
                        while *q != 0 && guard < 160 {
                            s.push(*q as char);
                            q = q.add(1);
                            guard += 1;
                        }
                        s
                    } else {
                        format!("0x{addr:x}")
                    };
                    eprintln!("  #{i:2} {name}");
                }
                CloseHandle(h);
            }
            SymCleanup(hproc);
            CloseHandle(hproc);
        }
    }
}

#[cfg(windows)]
fn main() {
    let pid: u32 = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .expect("usage: stackdump <pid>");
    imp::dump(pid);
}

#[cfg(not(windows))]
fn main() {}

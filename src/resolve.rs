use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_void;
use std::ptr;
use std::sync::{OnceLock, RwLock};
use libc::{in_addr_t, in_port_t, sockaddr_in};

extern "C" {
    fn dlsym(handle: *mut libc::c_void, symbol: *const libc::c_char) -> *mut libc::c_void;
}

const RTLD_NEXT: *mut libc::c_void = -1isize as *mut libc::c_void;

type GetAddrInfoFn = unsafe extern "C" fn(
    node: *const libc::c_char,
    service: *const libc::c_char,
    hints: *const libc::addrinfo,
    res: *mut *mut libc::addrinfo,
) -> libc::c_int;

static HOSTNAME_OVERRIDES: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();

fn get_hostname_overrides() -> &'static RwLock<HashMap<String, String>> {
    HOSTNAME_OVERRIDES.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn add_override(hostname: &str, hostname_override: &str) {
    let overrides = get_hostname_overrides();
    let mut overrides_lock = overrides.write().unwrap();

    overrides_lock.insert(hostname.to_string(), hostname_override.to_string());
}

#[no_mangle]
pub extern "C" fn getaddrinfo(
    node: *const libc::c_char,
    service: *const libc::c_char,
    hints: *const libc::addrinfo,
    res: *mut *mut libc::addrinfo,
) -> libc::c_int {
    unsafe {
        let symbol_name = CString::new("getaddrinfo").unwrap();
        let original_getaddrinfo = dlsym(RTLD_NEXT, symbol_name.as_ptr()) as *const GetAddrInfoFn;

        if original_getaddrinfo.is_null() {
            panic!("Failed to locate original getaddrinfo");
        }

        let original_getaddrinfo: GetAddrInfoFn = std::mem::transmute(original_getaddrinfo);

        let c_str = CStr::from_ptr(node);
        let hostname = c_str.to_str().unwrap();

        if let Some(addr) = get_hostname_overrides().read().unwrap().get(hostname) {
            let mut addrinfo_result: *mut libc::addrinfo = ptr::null_mut();
            let c_hostname = CString::new(addr.to_string()).unwrap();

            let result = original_getaddrinfo(c_hostname.as_ptr(), service, ptr::null(), &mut addrinfo_result);
            let mut ip: Option<libc::in_addr_t> = None;
            let mut port: Option<libc::in_port_t> = None;

            if result == 0 {
                let addr_tmp = &*addrinfo_result;
                let sockaddr = &*(addr_tmp.ai_addr as *const libc::sockaddr);

                if sockaddr.sa_family as i32 == libc::AF_INET {
                    let sockaddr_in = &*(addr_tmp.ai_addr as *const libc::sockaddr_in);

                    ip = Some(sockaddr_in.sin_addr.s_addr);
                    port = Some(sockaddr_in.sin_port);
                };

                libc::freeaddrinfo(addrinfo_result);
            }

            if ip.is_none() {
                return libc::EAI_NONAME;
            }

            let result = libc::malloc(size_of::<libc::addrinfo>()) as *mut libc::addrinfo;

            if result.is_null() {
                return libc::EAI_MEMORY;
            }

            ptr::write(result, libc::addrinfo {
                ai_flags: 0,
                ai_family: libc::AF_INET,
                ai_socktype: libc::SOCK_STREAM,
                ai_protocol: 0,
                ai_addrlen: size_of::<libc::sockaddr_in>() as u32,
                ai_canonname: ptr::null_mut(),
                ai_next: ptr::null_mut(),
                ai_addr: libc::malloc(size_of::<libc::sockaddr_in>()) as *mut libc::sockaddr,
            });

            if (*result).ai_addr.is_null() {
                libc::free(result as *mut c_void);

                return libc::EAI_MEMORY;
            }

            let sockaddr = (*result).ai_addr as *mut libc::sockaddr_in;

            write_sockaddr(sockaddr, ip, port);

            *res = result;

            return 0;
        }

        original_getaddrinfo(node, service, hints, res)
    }
}

#[cfg(target_os = "linux")]
unsafe fn write_sockaddr(sockaddr: *mut sockaddr_in, ip: Option<in_addr_t>, port: Option<in_port_t>) {
    ptr::write(sockaddr, libc::sockaddr_in {
        sin_family: libc::AF_INET as u16,
        sin_port: port.unwrap(),
        sin_addr: libc::in_addr {
            s_addr: ip.unwrap(),
        },
        sin_zero: [0; 8],
    });
}

#[cfg(target_os = "freebsd")]
unsafe fn write_sockaddr(sockaddr: *mut sockaddr_in, ip: Option<in_addr_t>, port: Option<in_port_t>) {
    ptr::write(sockaddr, libc::sockaddr_in {
        sin_len: 0,
        sin_family: libc::AF_INET as u8,
        sin_port: port.unwrap(),
        sin_addr: libc::in_addr {
            s_addr: ip.unwrap(),
        },
        sin_zero: [0; 8],
    });
}

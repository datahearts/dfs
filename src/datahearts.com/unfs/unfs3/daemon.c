
/*
 * UNFS3 server framework
 * Originally generated using rpcgen
 * Portions (C) 2004, Pascal Schmidt
 * see file LICENSE for license details
 */
#include <signal.h>
#include <rpc/svc.h>
#include <stdio.h>
#include <rpc/rpc.h>
#include <rpc/auth.h>
#include <errno.h>
#include <netinet/tcp.h>
#include "daemon.h"
#include "fh.c"
#include "xdr.c"
#include "attr.c"
#include "nfs.c"
#include "mount.c"

#define UNFS_NAME "UNFS3 to Golang Backend\n"

/* options and default values */
int opt_detach = FALSE;
char *opt_exports = "/etc/exports";
int opt_cluster = FALSE;
char *opt_cluster_path = "/";
int opt_tcponly = FALSE;
unsigned int opt_nfs_port = NFS_PORT;	/* 0 means RPC_ANYSOCK */
unsigned int opt_mount_port = NFS_PORT;
int opt_singleuser = TRUE;
int opt_brute_force = FALSE;
int opt_testconfig = FALSE;
struct in_addr opt_bind_addr;
int opt_readable_executables = FALSE;
char *opt_pid_file = NULL;

/* Register with portmapper? */
int opt_portmapper = TRUE;


/*
 * return remote address from svc_req structure
 */
struct in_addr get_remote(struct svc_req *rqstp)
{
    return (svc_getcaller(rqstp->rq_xprt))->sin_addr;
}

/*
 * return remote address's port from svc_req structure
 */
int get_remote_port
(struct svc_req *rqstp)
{
    return (svc_getcaller(rqstp->rq_xprt))->sin_port;
}

/*
 * return the socket type of the request (SOCK_STREAM or SOCK_DGRAM)
 */
int get_socket_type(struct svc_req *rqstp)
{
    int v, res;
    socklen_t l;

    l = sizeof(v);

#if HAVE_STRUCT___RPC_SVCXPRT_XP_FD == 1
    res = getsockopt(rqstp->rq_xprt->xp_fd, SOL_SOCKET, SO_TYPE, &v, &l);
#else
    res = getsockopt(rqstp->rq_xprt->xp_sock, SOL_SOCKET, SO_TYPE, &v, &l);
#endif

    if (res < 0) {
	fprintf(stderr, "unable to determine socket type\n");
	return -1;
    }

    return v;
}

/*
 * signal handler and error exit function
 */
void daemon_exit(int error)
{
    if (error == SIGUSR1) {
	return;
    }

    if (opt_portmapper) {
	svc_unregister(MOUNTPROG, MOUNTVERS1);
	svc_unregister(MOUNTPROG, MOUNTVERS3);
    }

    if (opt_portmapper) {
	svc_unregister(NFS3_PROGRAM, NFS_V3);
    }

    if (error == SIGSEGV)
	fprintf(stderr, "segmentation fault\n");

    if (opt_detach)
	closelog();

    go_shutdown();

    exit(1);
}

/*
 * NFS service dispatch function
 * generated by rpcgen
 */
static void nfs3_program_3(struct svc_req *rqstp, register SVCXPRT * transp)
{
    union {
	GETATTR3args nfsproc3_getattr_3_arg;
	SETATTR3args nfsproc3_setattr_3_arg;
	LOOKUP3args nfsproc3_lookup_3_arg;
	ACCESS3args nfsproc3_access_3_arg;
	READLINK3args nfsproc3_readlink_3_arg;
	READ3args nfsproc3_read_3_arg;
	WRITE3args nfsproc3_write_3_arg;
	CREATE3args nfsproc3_create_3_arg;
	MKDIR3args nfsproc3_mkdir_3_arg;
	SYMLINK3args nfsproc3_symlink_3_arg;
	MKNOD3args nfsproc3_mknod_3_arg;
	REMOVE3args nfsproc3_remove_3_arg;
	RMDIR3args nfsproc3_rmdir_3_arg;
	RENAME3args nfsproc3_rename_3_arg;
	LINK3args nfsproc3_link_3_arg;
	READDIR3args nfsproc3_readdir_3_arg;
	READDIRPLUS3args nfsproc3_readdirplus_3_arg;
	FSSTAT3args nfsproc3_fsstat_3_arg;
	FSINFO3args nfsproc3_fsinfo_3_arg;
	PATHCONF3args nfsproc3_pathconf_3_arg;
	COMMIT3args nfsproc3_commit_3_arg;
    } argument;
    char *result;
    xdrproc_t _xdr_argument, _xdr_result;
    char *(*local) (char *, struct svc_req *);
		
	//fprintf(stderr,  "NFS command %i\n", rqstp->rq_proc);
    switch (rqstp->rq_proc) {
	case NFSPROC3_NULL:
	    _xdr_argument = (xdrproc_t) xdr_void;
	    _xdr_result = (xdrproc_t) xdr_void;
	    local = (char *(*)(char *, struct svc_req *)) nfsproc3_null_3_svc;
	    break;

	case NFSPROC3_GETATTR:
	    _xdr_argument = (xdrproc_t) xdr_GETATTR3args;
	    _xdr_result = (xdrproc_t) xdr_GETATTR3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_getattr_3_svc;
	    break;

	case NFSPROC3_SETATTR:
	    _xdr_argument = (xdrproc_t) xdr_SETATTR3args;
	    _xdr_result = (xdrproc_t) xdr_SETATTR3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_setattr_3_svc;
	    break;

	case NFSPROC3_LOOKUP:
	    _xdr_argument = (xdrproc_t) xdr_LOOKUP3args;
	    _xdr_result = (xdrproc_t) xdr_LOOKUP3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_lookup_3_svc;
	    break;

	case NFSPROC3_ACCESS:
	    _xdr_argument = (xdrproc_t) xdr_ACCESS3args;
	    _xdr_result = (xdrproc_t) xdr_ACCESS3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_access_3_svc;
	    break;

	case NFSPROC3_READLINK:
	    _xdr_argument = (xdrproc_t) xdr_READLINK3args;
	    _xdr_result = (xdrproc_t) xdr_READLINK3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_readlink_3_svc;
	    break;

	case NFSPROC3_READ:
	    _xdr_argument = (xdrproc_t) xdr_READ3args;
	    _xdr_result = (xdrproc_t) xdr_READ3res;
	    local = (char *(*)(char *, struct svc_req *)) nfsproc3_read_3_svc;
	    break;

	case NFSPROC3_WRITE:
	    _xdr_argument = (xdrproc_t) xdr_WRITE3args;
	    _xdr_result = (xdrproc_t) xdr_WRITE3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_write_3_svc;
	    break;

	case NFSPROC3_CREATE:
	    _xdr_argument = (xdrproc_t) xdr_CREATE3args;
	    _xdr_result = (xdrproc_t) xdr_CREATE3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_create_3_svc;
	    break;

	case NFSPROC3_MKDIR:
	    _xdr_argument = (xdrproc_t) xdr_MKDIR3args;
	    _xdr_result = (xdrproc_t) xdr_MKDIR3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_mkdir_3_svc;
	    break;

	case NFSPROC3_SYMLINK:
	    _xdr_argument = (xdrproc_t) xdr_SYMLINK3args;
	    _xdr_result = (xdrproc_t) xdr_SYMLINK3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_symlink_3_svc;
	    break;

	case NFSPROC3_MKNOD:
	    _xdr_argument = (xdrproc_t) xdr_MKNOD3args;
	    _xdr_result = (xdrproc_t) xdr_MKNOD3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_mknod_3_svc;
	    break;

	case NFSPROC3_REMOVE:
	    _xdr_argument = (xdrproc_t) xdr_REMOVE3args;
	    _xdr_result = (xdrproc_t) xdr_REMOVE3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_remove_3_svc;
	    break;

	case NFSPROC3_RMDIR:
	    _xdr_argument = (xdrproc_t) xdr_RMDIR3args;
	    _xdr_result = (xdrproc_t) xdr_RMDIR3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_rmdir_3_svc;
	    break;

	case NFSPROC3_RENAME:
	    _xdr_argument = (xdrproc_t) xdr_RENAME3args;
	    _xdr_result = (xdrproc_t) xdr_RENAME3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_rename_3_svc;
	    break;

	case NFSPROC3_LINK:
	    _xdr_argument = (xdrproc_t) xdr_LINK3args;
	    _xdr_result = (xdrproc_t) xdr_LINK3res;
	    local = (char *(*)(char *, struct svc_req *)) nfsproc3_link_3_svc;
	    break;

	case NFSPROC3_READDIR:
	    _xdr_argument = (xdrproc_t) xdr_READDIR3args;
	    _xdr_result = (xdrproc_t) xdr_READDIR3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_readdir_3_svc;
	    break;

	case NFSPROC3_READDIRPLUS:
	    _xdr_argument = (xdrproc_t) xdr_READDIRPLUS3args;
	    _xdr_result = (xdrproc_t) xdr_READDIRPLUS3res;
	    local = (char *(*)(char *, struct svc_req *))
		nfsproc3_readdirplus_3_svc;
	    break;

	case NFSPROC3_FSSTAT:
	    _xdr_argument = (xdrproc_t) xdr_FSSTAT3args;
	    _xdr_result = (xdrproc_t) xdr_FSSTAT3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_fsstat_3_svc;
	    break;

	case NFSPROC3_FSINFO:
	    _xdr_argument = (xdrproc_t) xdr_FSINFO3args;
	    _xdr_result = (xdrproc_t) xdr_FSINFO3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_fsinfo_3_svc;
	    break;

	case NFSPROC3_PATHCONF:
	    _xdr_argument = (xdrproc_t) xdr_PATHCONF3args;
	    _xdr_result = (xdrproc_t) xdr_PATHCONF3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_pathconf_3_svc;
	    break;

	case NFSPROC3_COMMIT:
	    _xdr_argument = (xdrproc_t) xdr_COMMIT3args;
	    _xdr_result = (xdrproc_t) xdr_COMMIT3res;
	    local =
		(char *(*)(char *, struct svc_req *)) nfsproc3_commit_3_svc;
	    break;

	default:
	    svcerr_noproc(transp);
	    return;
    }
    memset((char *) &argument, 0, sizeof(argument));
    if (!svc_getargs(transp, (xdrproc_t) _xdr_argument, (caddr_t) & argument)) {
	svcerr_decode(transp);
	return;
    }
    result = (*local) ((char *) &argument, rqstp);
    if (result != NULL &&
	!svc_sendreply(transp, (xdrproc_t) _xdr_result, result)) {
		svcerr_systemerr(transp);
		fprintf(stderr, "%s\n", "unable to send NFS RPC reply");
	}
    if (!svc_freeargs (transp, (xdrproc_t) _xdr_argument, (caddr_t) & argument)) {
		fprintf(stderr, "%s\n", "unable to free NFS XDR arguments");
	}
    return;
}
 
/*
 * mount protocol dispatcher
 * generated by rpcgen
 */
static void mountprog_3(struct svc_req *rqstp, register SVCXPRT * transp)
{
    union {
	dirpath mountproc_mnt_3_arg;
	dirpath mountproc_umnt_3_arg;
    } argument;
    char *result;
    xdrproc_t _xdr_argument, _xdr_result;
    char *(*local) (char *, struct svc_req *);

	//fprintf(stderr,  "Mount command %i\n", rqstp->rq_proc);
	
    switch (rqstp->rq_proc) {
	case MOUNTPROC_NULL:
	    _xdr_argument = (xdrproc_t) xdr_void;
	    _xdr_result = (xdrproc_t) xdr_void;
	    local =
		(char *(*)(char *, struct svc_req *)) mountproc_null_3_svc;
	    break;

	case MOUNTPROC_MNT:
	    _xdr_argument = (xdrproc_t) xdr_dirpath;
	    _xdr_result = (xdrproc_t) xdr_mountres3;
	    local = (char *(*)(char *, struct svc_req *)) mountproc_mnt_3_svc;
	    break;

	case MOUNTPROC_DUMP:
	    _xdr_argument = (xdrproc_t) xdr_void;
	    _xdr_result = (xdrproc_t) xdr_mountlist;
	    local =
		(char *(*)(char *, struct svc_req *)) mountproc_dump_3_svc;
	    break;

	case MOUNTPROC_UMNT:
	    _xdr_argument = (xdrproc_t) xdr_dirpath;
	    _xdr_result = (xdrproc_t) xdr_void;
	    local =
		(char *(*)(char *, struct svc_req *)) mountproc_umnt_3_svc;
	    break;

	case MOUNTPROC_UMNTALL:
	    _xdr_argument = (xdrproc_t) xdr_void;
	    _xdr_result = (xdrproc_t) xdr_void;
	    local =
		(char *(*)(char *, struct svc_req *)) mountproc_umntall_3_svc;
	    break;

	case MOUNTPROC_EXPORT:
	    _xdr_argument = (xdrproc_t) xdr_void;
	    _xdr_result = (xdrproc_t) xdr_exports;
	    local = (char *(*)(char *, struct svc_req *)) mountproc_export_3_svc;
	    break;

	default:
	    svcerr_noproc(transp);
	    return;
    }
    memset((char *) &argument, 0, sizeof(argument));
    if (!svc_getargs(transp, (xdrproc_t) _xdr_argument, (caddr_t) & argument)) {
	svcerr_decode(transp);
	return;
    }
    result = (*local) ((char *) &argument, rqstp);
    if (result != NULL &&
	!svc_sendreply(transp, (xdrproc_t) _xdr_result, result)) {
		svcerr_systemerr(transp);
		fprintf(stderr, "unable to send Mount RPC reply\n");
    }
    if (!svc_freeargs (transp, (xdrproc_t) _xdr_argument, (caddr_t) & argument)) {
		fprintf(stderr, "unable to free Mount XDR arguments\n");
    }
    return;
}
 
static void register_nfs_service(SVCXPRT * udptransp, SVCXPRT * tcptransp)
{
    if (opt_portmapper) {
	pmap_unset(NFS3_PROGRAM, NFS_V3);
    }

    if (udptransp != NULL) {
	/* Register NFS service for UDP */
	if (!svc_register
	    (udptransp, NFS3_PROGRAM, NFS_V3, nfs3_program_3,
	     opt_portmapper ? IPPROTO_UDP : 0)) {
	    fprintf(stderr, "%s\n",
		    "unable to register (NFS3_PROGRAM, NFS_V3, udp).");
	    daemon_exit(0);
	}
    }

    if (tcptransp != NULL) {
	/* Register NFS service for TCP */
	if (!svc_register
	    (tcptransp, NFS3_PROGRAM, NFS_V3, nfs3_program_3,
	     opt_portmapper ? IPPROTO_TCP : 0)) {
	    fprintf(stderr, "%s\n",
		    "unable to register (NFS3_PROGRAM, NFS_V3, tcp).");
	    daemon_exit(0);
	}
    }
}

static void register_mount_service(SVCXPRT * udptransp, SVCXPRT * tcptransp)
{
    if (opt_portmapper) {
	pmap_unset(MOUNTPROG, MOUNTVERS1);
	pmap_unset(MOUNTPROG, MOUNTVERS3);
    }

    if (udptransp != NULL) {
	/* Register MOUNT service (v1) for UDP */
	if (!svc_register
	    (udptransp, MOUNTPROG, MOUNTVERS1, mountprog_3,
	     opt_portmapper ? IPPROTO_UDP : 0)) {
	    fprintf(stderr, "%s\n",
		    "unable to register (MOUNTPROG, MOUNTVERS1, udp).");
	    daemon_exit(0);
	}

	/* Register MOUNT service (v3) for UDP */
	if (!svc_register
	    (udptransp, MOUNTPROG, MOUNTVERS3, mountprog_3,
	     opt_portmapper ? IPPROTO_UDP : 0)) {
	    fprintf(stderr, "%s\n",
		    "unable to register (MOUNTPROG, MOUNTVERS3, udp).");
	    daemon_exit(0);
	}
    }

    if (tcptransp != NULL) {
	/* Register MOUNT service (v1) for TCP */
	if (!svc_register
	    (tcptransp, MOUNTPROG, MOUNTVERS1, mountprog_3,
	     opt_portmapper ? IPPROTO_TCP : 0)) {
	    fprintf(stderr, "%s\n",
		    "unable to register (MOUNTPROG, MOUNTVERS1, tcp).");
	    daemon_exit(0);
	}

	/* Register MOUNT service (v3) for TCP */
	if (!svc_register
	    (tcptransp, MOUNTPROG, MOUNTVERS3, mountprog_3,
	     opt_portmapper ? IPPROTO_TCP : 0)) {
	    fprintf(stderr, "%s\n",
		    "unable to register (MOUNTPROG, MOUNTVERS3, tcp).");
	    daemon_exit(0);
	}
    }
}

static SVCXPRT *create_udp_transport(unsigned int port)
{
    SVCXPRT *transp = NULL;
    struct sockaddr_in sin;
    int sock;
    const int on = 1;

    /* Make sure we null the entire sockaddr_in structure */
    memset(&sin, 0, sizeof(struct sockaddr_in));

    if (port == 0)
	sock = RPC_ANYSOCK;
    else {
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr.s_addr = opt_bind_addr.s_addr;
	sock = socket(PF_INET, SOCK_DGRAM, 0);
	setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (const char *) &on, sizeof(on));
	if (bind(sock, (struct sockaddr *) &sin, sizeof(struct sockaddr))) {
	    perror("bind");
	    fprintf(stderr, "Couldn't bind to udp port %d\n", port);
	    exit(1);
	}
    }

    transp = svcudp_bufcreate(sock, NFS_MAX_UDP_PACKET, NFS_MAX_UDP_PACKET);

    if (transp == NULL) {
	fprintf(stderr, "%s\n", "cannot create udp service.");
	daemon_exit(0);
    }

    return transp;
}

static SVCXPRT *create_tcp_transport(unsigned int port)
{
    SVCXPRT *transp = NULL;
    struct sockaddr_in sin;
    int sock;
    const int on = 1;

    /* Make sure we null the entire sockaddr_in structure */
    memset(&sin, 0, sizeof(struct sockaddr_in));

    if (port == 0)
	sock = RPC_ANYSOCK;
    else {
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr.s_addr = opt_bind_addr.s_addr;
	sock = socket(PF_INET, SOCK_STREAM, 0);
	setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (const char *) &on, sizeof(on));
    setsockopt(sock,IPPROTO_TCP,TCP_NODELAY,(char*)&on,sizeof(on));
	if (bind(sock, (struct sockaddr *) &sin, sizeof(struct sockaddr))) {
	    perror("bind");
	    fprintf(stderr, "Couldn't bind to tcp port %d\n", port);
	    exit(1);
	}
    }

    transp = svctcp_create(sock, 0, 0);

    if (transp == NULL) {
	fprintf(stderr, "%s\n", "cannot create tcp service.");
	daemon_exit(0);
    }

    return transp;
}

/* Run RPC service. This is our own implementation of svc_run(), which
   allows us to handle other events as well. */
static void unfs3_svc_run(void)
{
#ifdef HAVE_SVC_GETREQ_POLL
    int r;
#else
    fd_set readfds;
    struct timeval tv;
#endif

    for (;;) {

#ifdef HAVE_SVC_GETREQ_POLL
	r = poll(svc_pollfd, svc_max_pollfd, 2*1000);
	if (r < 0) {
		if (errno == EINTR) {
		    continue;
		}
		perror("unfs3_svc_run: poll failed");
		return;
	}
	else if (r)
		svc_getreq_poll(svc_pollfd, r);

#else
	readfds = svc_fdset;
	tv.tv_sec = 1;
	tv.tv_usec = 0;
	/* Note: On Windows, it's not possible to call select with all sets
	   empty; to use it as a sleep function. In our case, however,
	   readfds should never be empty, since we always have our listen
	   socket. Well, at least hope that our Windows RPC library works
	   like that. oncrpc-ms does. */
	switch (select(FD_SETSIZE, &readfds, NULL, NULL, &tv)) {
	    case -1:
		if (errno == EINTR) {
		    continue;
		}
		perror("unfs3_svc_run: select failed");
		return;
	    case 0:
		/* timeout */
		continue;
	    default:
		svc_getreqset(&readfds);
	}
#endif
    }
}

static void start(void) {
	//printf("start\n");
	register SVCXPRT *tcptransp = NULL, *udptransp = NULL;
    go_init();
	//printf("backend inited\n");
	setvbuf(stdout, NULL, _IOLBF, 0);
	udptransp = create_udp_transport(2049);
    tcptransp = create_tcp_transport(2049);
	//printf("transports created\n");
	register_nfs_service(udptransp, tcptransp);
    register_mount_service(udptransp, tcptransp);
	//printf("services registered, about to hit main loop\n");
	unfs3_svc_run();
}


/*
 * UNFS3 NFS protocol procedures
 * (C) 2004, Pascal Schmidt
 * Copyright 2014 Karl Mikaelsson <derfian@cendio.se> for Cendio AB
 * see file LICENSE for license details
 */

#include <arpa/inet.h> //inet_ntoa

/*
 * cat an object name onto a path, checking for illegal input
 */
nfsstat3 cat_name(const char *path, const char *name, char *result)
{
    char *last;

    if (!path)
	return NFS3ERR_STALE;

    if (!name)
	return NFS3ERR_ACCES;

    if (name[0] == 0 || strchr(name, '/') != NULL)
	return NFS3ERR_ACCES;

    if (strlen(path) + strlen(name) + 2 > NFS_MAXPATHLEN)
	return NFS3ERR_NAMETOOLONG;

    if (strcmp(name, ".") == 0) {
	strcpy(result, path);
	return NFS3_OK;
    }

    sprintf(result, "%s/%s", path, name);
    return NFS3_OK;
}

void *nfsproc3_null_3_svc(U(void *argp), U(struct svc_req *rqstp))
{
    static void *result = NULL;

    return &result;
}

GETATTR3res *nfsproc3_getattr_3_svc(GETATTR3args * argp,
				    struct svc_req * rqstp)
{
    static GETATTR3res result;
    uint64 ino;
    post_op_attr post;

    ino = fh_decomp(argp->object);
    post = get_post(ino, "", rqstp);

    result.status = NFS3_OK;
    result.GETATTR3res_u.resok.obj_attributes = post.post_op_attr_u.attributes;

    return &result;
}

SETATTR3res *nfsproc3_setattr_3_svc(SETATTR3args * argp, struct svc_req * rqstp)
{
    static SETATTR3res result;
    pre_op_attr pre;
    uint64 ino;
	int sres =NFS3_OK, mres =NFS3_OK, mtres =NFS3_OK;
	sattr3 new = argp->new_attributes;
    ino = fh_decomp(argp->object);
    pre = get_pre(ino);

    /* set file size? */
    if (new.size.set_it == TRUE) {
		sres = go_truncate(ino, new.size.set_size3_u.size);
    } 
	
    /* set file mode? */
	if (new.mode.set_it == TRUE) {
		mres = go_chmod(ino, new.mode.set_mode3_u.mode);
    }
	
    /* set file modtime? */
	if (new.mtime.set_it != DONT_CHANGE) {
		if (new.mtime.set_it == SET_TO_SERVER_TIME)
			mtres = go_modtime(ino, time(NULL));
		else			       /* SET_TO_CLIENT_TIME */
			mtres = go_modtime(ino, new.mtime.set_mtime_u.mtime.seconds);
	}
		
	result.status = (sres != NFS3_OK) ? sres : (mres != NFS3_OK) ? mres : mtres;
	
    /* overlaps with resfail */
    result.SETATTR3res_u.resok.obj_wcc.before = pre;
    result.SETATTR3res_u.resok.obj_wcc.after = get_post(ino, "", rqstp);
    return &result;
}

LOOKUP3res *nfsproc3_lookup_3_svc(LOOKUP3args * argp, struct svc_req * rqstp)
{
    static LOOKUP3res result;
    unfs3_fh_t *fh;
    uint64 ino;
    go_statstruct buf;

	ino = fh_decomp(argp->what.dir);
		result.status = go_lstat(ino, argp->what.name, &buf);
		if (result.status == NFS3_OK) {
			fh = fh_comp(buf.st_ino, argp->what.name);
			if (fh) {
				result.LOOKUP3res_u.resok.object.data.data_len = fh_length(fh);
				result.LOOKUP3res_u.resok.object.data.data_val = (char *) fh;
				result.LOOKUP3res_u.resok.obj_attributes = get_post_buf(buf, rqstp);
			} else {
				result.status = NFS3ERR_NAMETOOLONG;
			}
		}
	
	/* overlaps with resfail */
    result.LOOKUP3res_u.resok.dir_attributes = get_post(ino, "", rqstp);
    return &result;
}

ACCESS3res *nfsproc3_access_3_svc(ACCESS3args * argp, struct svc_req * rqstp)
{
    static ACCESS3res result;
    uint64 ino;
    post_op_attr post;
    int newaccess = 0;

    ino = fh_decomp(argp->object);
	
	go_statstruct buf;
	result.status = go_lstat(ino, "", &buf);
    if (result.status==NFS3_OK) {
		post = get_post_buf(buf, rqstp);
		//TODO: Fill this out based on the stated info in 'buf'
		/* allow everything */
		newaccess |= ACCESS3_READ | ACCESS3_MODIFY | ACCESS3_EXTEND | ACCESS3_EXECUTE;

		/* adjust if directory */
		if (post.post_op_attr_u.attributes.type == NF3DIR) {
			if (newaccess & (ACCESS3_READ | ACCESS3_EXECUTE))
				newaccess |= ACCESS3_LOOKUP;
			if (newaccess & ACCESS3_MODIFY)
				newaccess |= ACCESS3_DELETE;
			newaccess &= ~ACCESS3_EXECUTE;
		}
	} else {
		post = error_attr;
	}
	
    result.ACCESS3res_u.resok.access = newaccess;
    result.ACCESS3res_u.resok.obj_attributes = post;
	
    return &result;
}

READLINK3res *nfsproc3_readlink_3_svc(READLINK3args * argp, struct svc_req * rqstp)
{	//TODO: test that this is being rejected correctly
    static READLINK3res result;
    result.status = NFS3ERR_NOTSUPP;
    
    result.READLINK3res_u.resfail.symlink_attributes.attributes_follow = FALSE;

    return &result;
}

READ3res *nfsproc3_read_3_svc(READ3args * argp, struct svc_req * rqstp)
{
    static READ3res result;
    uint64 ino;
    int res;
    static char buf[NFS_MAXDATA_TCP + 1];
    unsigned int maxdata;

    if (get_socket_type(rqstp) == SOCK_STREAM)
	maxdata = NFS_MAXDATA_TCP;
    else
	maxdata = NFS_MAXDATA_UDP;

    ino = fh_decomp(argp->file);

    /* if bigger than rtmax, truncate length */
    if (argp->count > maxdata)
	argp->count = maxdata;

	/* read one more to check for eof */
    res = go_pread(ino, buf, argp->count, argp->offset);
	if (res > -1) {
		result.status = NFS3_OK;

	    /* eof if we could not read one more */
	    result.READ3res_u.resok.eof = (res < (int64) argp->count);
		
		result.READ3res_u.resok.count = res;
		result.READ3res_u.resok.data.data_len = res;
		result.READ3res_u.resok.data.data_val = buf;
	} else {
		//because a successful pread can return any non-negative number
		//it can't return standard NF3 errors (which are all positive)
		//so it sends them as a negative to indicate it's an error,
		//and we have to negative it again here to get the original error.
			result.status = -res;
	}

    /* overlaps with resfail */
    result.READ3res_u.resok.file_attributes = get_post(ino, "", rqstp);
    return &result;
}

WRITE3res *nfsproc3_write_3_svc(WRITE3args * argp, struct svc_req * rqstp)
{
    static WRITE3res result;
    uint64 ino;
    int res;
	pre_op_attr pre;
	
	ino = fh_decomp(argp->file);
	
	pre = get_pre(ino);
	res = go_pwrite(ino, argp->data.data_val, argp->data.data_len, argp->offset);
    if (res > -1) {
		result.status = NFS3_OK;
		result.WRITE3res_u.resok.count = res;
		result.WRITE3res_u.resok.committed = FILE_SYNC;
		uint64 zero = (uint64) 0;
		memcpy(result.WRITE3res_u.resok.verf, &zero, NFS3_WRITEVERFSIZE);
    } else {
		//because a successful pwrite can return any non-negative number
		//it can't return standard NF3 errors (which are all positive)
		//so it sends them as a negative to indicate it's an error,
		//and we have to negative it again here to get the original error.
		result.status = -res;
	}

    /* overlaps with resfail */
    result.WRITE3res_u.resok.file_wcc.before = pre;
    result.WRITE3res_u.resok.file_wcc.after = get_post(ino, "", rqstp);
    return &result;
}

CREATE3res *nfsproc3_create_3_svc(CREATE3args * argp, struct svc_req * rqstp)
{
    static CREATE3res result;
    uint64 ino;
    sattr3 new_attr;
    go_statstruct buf;

	ino = fh_decomp(argp->where.dir);
	pre_op_attr pre;
	pre = get_pre(ino);
    if (argp->how.mode != EXCLUSIVE) {
	new_attr = argp->how.createhow3_u.obj_attributes;
    }

	if (argp->how.mode == UNCHECKED) { //overwrite already if exists
		result.status = go_createover(ino, argp->where.name, get_rpcinfo(rqstp, get_mode(new_attr)));
	} else {
		result.status = go_create(ino, argp->where.name, get_rpcinfo(rqstp, get_mode(new_attr)));
	    }

	if (result.status ==  NFS3_OK) {
			result.status = go_lstat(ino, argp->where.name, &buf);
			result.CREATE3res_u.resok.obj = fh_comp_post(buf.st_ino, argp->where.name);
			result.CREATE3res_u.resok.obj_attributes = get_post_buf(buf, rqstp);
    }

	/*"overlaps with resfail*/
    result.CREATE3res_u.resok.dir_wcc.before = pre;
    result.CREATE3res_u.resok.dir_wcc.after = get_post(ino, "", rqstp);

    return &result;
}

MKDIR3res *nfsproc3_mkdir_3_svc(MKDIR3args * argp, struct svc_req * rqstp)
{
    static MKDIR3res result;
    uint64 ino;
    pre_op_attr pre;

    ino = fh_decomp(argp->where.dir);
    pre = get_pre(ino);

	result.status = go_mkdir(ino, argp->where.name, get_rpcinfo(rqstp, get_mode(argp->attributes)));
	if (result.status == NFS3_OK){
		result.MKDIR3res_u.resok.obj = fh_comp_type(ino, argp->where.name, S_IFDIR);
		result.MKDIR3res_u.resok.obj_attributes = get_post(ino, argp->where.name, rqstp);
	}

    /* overlaps with resfail */
    result.MKDIR3res_u.resok.dir_wcc.before = pre;
    result.MKDIR3res_u.resok.dir_wcc.after = get_post(ino, "", rqstp);

    return &result;
}

SYMLINK3res *nfsproc3_symlink_3_svc(SYMLINK3args * argp, struct svc_req * rqstp)
{	//TODO: test that this is being rejected correctly
    static SYMLINK3res result;
    result.status = NFS3ERR_NOTSUPP;

    result.SYMLINK3res_u.resfail.dir_wcc.before.attributes_follow = FALSE;
    result.SYMLINK3res_u.resfail.dir_wcc.after.attributes_follow = FALSE;

    return &result;
}

MKNOD3res *nfsproc3_mknod_3_svc(MKNOD3args * argp, struct svc_req * rqstp)
{	//TODO: test that this is being rejected correctly
    static MKNOD3res result;
    result.status = NFS3ERR_NOTSUPP;
	
    result.MKNOD3res_u.resfail.dir_wcc.before.attributes_follow = FALSE;
    result.MKNOD3res_u.resfail.dir_wcc.after.attributes_follow = FALSE;

    return &result;
}

REMOVE3res *nfsproc3_remove_3_svc(REMOVE3args * argp, struct svc_req * rqstp)
{
    static REMOVE3res result;
    uint64 ino;

    ino = fh_decomp(argp->object.dir);
	pre_op_attr pre;
	pre = get_pre(ino);
    
	result.status = go_remove(ino, argp->object.name);

    /* overlaps with resfail */
    result.REMOVE3res_u.resok.dir_wcc.before = pre;
    result.REMOVE3res_u.resok.dir_wcc.after = get_post(ino, "", rqstp);
    return &result;
}

RMDIR3res *nfsproc3_rmdir_3_svc(RMDIR3args * argp, struct svc_req * rqstp)
{
    static RMDIR3res result;
    uint64 ino;
	pre_op_attr pre;

    ino = fh_decomp(argp->object.dir);
	pre = get_pre(ino);
    
    result.status = go_rmdir(ino, argp->object.name);

    /* overlaps with resfail */
    result.RMDIR3res_u.resok.dir_wcc.before = pre;
    result.RMDIR3res_u.resok.dir_wcc.after = get_post(ino, "", rqstp);
    return &result;
}

RENAME3res *nfsproc3_rename_3_svc(RENAME3args * argp, struct svc_req * rqstp)
{
    static RENAME3res result;
    uint64 from;
    uint64 to;
    post_op_attr post;

    from = fh_decomp(argp->from.dir);
	
    pre_op_attr from_pre;
    from_pre = get_pre(from);
	
    to = fh_decomp(argp->to.dir);
	
	pre_op_attr to_pre;
	to_pre = get_pre(to);

    result.status = go_rename(from, argp->from.name, to, argp->to.name);

    post = get_post(from, "", rqstp);

    /* overlaps with resfail */
    result.RENAME3res_u.resok.fromdir_wcc.before = from_pre;
    result.RENAME3res_u.resok.fromdir_wcc.after = post;
    result.RENAME3res_u.resok.todir_wcc.before = to_pre;
    result.RENAME3res_u.resok.todir_wcc.after = get_post(to, "", rqstp);

    return &result;
}

LINK3res *nfsproc3_link_3_svc(LINK3args * argp, struct svc_req * rqstp)
{	//TODO: test that this is being rejected correctly
    static LINK3res result;
	result.status = NFS3ERR_NOTSUPP;

    result.LINK3res_u.resfail.file_attributes.attributes_follow = FALSE;
    result.LINK3res_u.resfail.linkdir_wcc.before.attributes_follow = FALSE;
    result.LINK3res_u.resfail.linkdir_wcc.after.attributes_follow = FALSE;

    return &result;
}

READDIR3res *nfsproc3_readdir_3_svc(READDIR3args * argp, struct svc_req * rqstp)
{
    static READDIR3res result;
    uint64 ino;	
    ino = fh_decomp(argp->dir);
	int res;
	READDIR3resok resok;
    static entry3 entries[MAX_ENTRIES];
    count3 count;
    static char names[NFS_MAXPATHLEN * MAX_ENTRIES];

	count = (argp->count);
    /* we refuse to return more than 4k from READDIR */
    //if (count > 4096)
	//count = 4096;

    /* account for size of information heading resok structure */
    //count -= RESOK_SIZE;
	
	res = go_readdir_full(ino, argp->cookie, count, names, entries, NFS_MAXPATHLEN, MAX_ENTRIES);
	
	//if OK, but didn't read the end of the directory, we get back a negative signal
	if (res<0) {
		res = NFS3_OK;
		resok.reply.eof = FALSE;
	} else if (res == NFS3_OK){
		resok.reply.eof = TRUE;	
	}
	
	result.status = res;
	
	if (entries[0].name)
		resok.reply.entries = &entries[0];
    else
		resok.reply.entries = NULL;

    uint64 zero = (uint64) 0;
	memcpy(resok.cookieverf, &zero, NFS3_COOKIEVERFSIZE);

    result.READDIR3res_u.resok = resok;	
    result.READDIR3res_u.resok.dir_attributes = get_post(ino, "", rqstp);

    return &result;
}

READDIRPLUS3res *nfsproc3_readdirplus_3_svc(U(READDIRPLUS3args * argp), U(struct svc_req * rqstp))
{
    static READDIRPLUS3res result;

    /* 
     * we don't do READDIRPLUS since it involves filehandle and
     * attribute getting which is impossible to do atomically
     * from user-space
     */
    result.status = NFS3ERR_NOTSUPP;
    result.READDIRPLUS3res_u.resfail.dir_attributes.attributes_follow = FALSE;

    return &result;
}

FSSTAT3res *nfsproc3_fsstat_3_svc(FSSTAT3args * argp, struct svc_req * rqstp)
{
    static FSSTAT3res result;
    uint64 ino;
    FSSTAT3resok size;
    ino = fh_decomp(argp->fsroot);

    /* overlaps with resfail */
    result.FSSTAT3res_u.resok.obj_attributes = get_post(ino, "", rqstp);

    result.status = go_fsstat(ino, &size);
    result.FSSTAT3res_u.resok.tbytes = size.tbytes;
    result.FSSTAT3res_u.resok.fbytes = size.fbytes;
    result.FSSTAT3res_u.resok.abytes = size.abytes;
    result.FSSTAT3res_u.resok.tfiles = INT_MAX;
    result.FSSTAT3res_u.resok.ffiles = INT_MAX;
    result.FSSTAT3res_u.resok.afiles = INT_MAX;
    result.FSSTAT3res_u.resok.invarsec = 0;

    return &result;
}

FSINFO3res *nfsproc3_fsinfo_3_svc(FSINFO3args * argp, struct svc_req * rqstp)
{
    static FSINFO3res result;
    uint64 ino;
    unsigned int maxdata;

    if (get_socket_type(rqstp) == SOCK_STREAM)
	maxdata = NFS_MAXDATA_TCP;
    else
	maxdata = NFS_MAXDATA_UDP;

    ino = fh_decomp(argp->fsroot);

    result.FSINFO3res_u.resok.obj_attributes = get_post(ino, "", rqstp);

    result.status = NFS3_OK;
    result.FSINFO3res_u.resok.rtmax = maxdata;
    result.FSINFO3res_u.resok.rtpref = maxdata;
    result.FSINFO3res_u.resok.rtmult = 4096;
    result.FSINFO3res_u.resok.wtmax = maxdata;
    result.FSINFO3res_u.resok.wtpref = maxdata;
    result.FSINFO3res_u.resok.wtmult = 4096;
    result.FSINFO3res_u.resok.dtpref = 4096;
    result.FSINFO3res_u.resok.maxfilesize = ~0ULL;
    result.FSINFO3res_u.resok.time_delta.seconds = 1;
    result.FSINFO3res_u.resok.time_delta.nseconds = 0;
    result.FSINFO3res_u.resok.properties = FSF3_LINK | FSF3_SYMLINK | FSF3_HOMOGENEOUS | FSF3_CANSETTIME;

    return &result;
}

PATHCONF3res *nfsproc3_pathconf_3_svc(PATHCONF3args * argp, struct svc_req * rqstp)
{
    static PATHCONF3res result;
    uint64 ino;

    ino = fh_decomp(argp->object);

    result.PATHCONF3res_u.resok.obj_attributes = get_post(ino, "", rqstp);

    result.status = NFS3_OK;
    result.PATHCONF3res_u.resok.linkmax = 0xFFFFFFFF;
    result.PATHCONF3res_u.resok.name_max = NFS_MAXPATHLEN;
    result.PATHCONF3res_u.resok.no_trunc = TRUE;
    result.PATHCONF3res_u.resok.chown_restricted = FALSE;
    result.PATHCONF3res_u.resok.case_insensitive = FALSE;
    result.PATHCONF3res_u.resok.case_preserving = TRUE;

    return &result;
}

COMMIT3res *nfsproc3_commit_3_svc(COMMIT3args * argp, struct svc_req * rqstp)
{
    static COMMIT3res result;
    uint64 ino;
    go_statstruct buf;
	pre_op_attr poa;
    ino = fh_decomp(argp->file);
	
	result.status = go_sync(ino, &buf);
		
    if (result.status == NFS3_OK) {
		uint64 zero = (uint64) 0;
		memcpy(result.COMMIT3res_u.resok.verf, &zero, NFS3_WRITEVERFSIZE);
    /* overlaps with resfail */
    result.COMMIT3res_u.resfail.file_wcc.before = get_pre_buf(buf);
    result.COMMIT3res_u.resfail.file_wcc.after = get_post_buf(buf, rqstp);
	} else {
		poa.attributes_follow = FALSE;
		result.COMMIT3res_u.resfail.file_wcc.before = poa;
		result.COMMIT3res_u.resfail.file_wcc.after = get_post_err();
	}

    return &result;
}

rpcinfo get_rpcinfo(struct svc_req * rqstp, mode_t mode)
{
	rpcinfo rpcinfo;
	rpcinfo.host = inet_ntoa(get_remote(rqstp));
	rpcinfo.port = get_remote_port(rqstp);
	rpcinfo.mode = mode;
	if (rqstp->rq_cred.oa_flavor == AUTH_UNIX)
	{
		struct authunix_parms * auth = (struct authunix_parms *)rqstp->rq_clntcred;
		rpcinfo.uid = auth->aup_uid;
		rpcinfo.gid = auth->aup_gid;
	}

	return rpcinfo;
}

#include <inittypes.h>
#include <sys/types.h>
#include <unistd.h>

static char *
glusterd_zfs_snapshot_device (char *snapname, int32_t brickcount,
                              glusterd_brickinfo_t  *brickinfo)
{
	char        snap[PATH_MAX]      = "";
	char        msg[1024]           = "";
	char        zpool[PATH_MAX]     = "";
	char       *snap_device         = NULL;
	xlator_t   *this                = NULL;
	runner_t    runner              = {0,};
	char       *ptr                 = NULL;
	int         ret                 = -1;

	this = THIS;
	GF_ASSERT (this);
	if (!snapname) {
		gf_log (this->name, GF_LOG_ERROR, "snapname is NULL");
		goto out;
	}

	runinit (&runner);
	snprintf (msg, sizeof (msg), "running zfs command, "
			"for getting zfs pool name from brick path");
	runner_add_args (&runner, "zfs", "list", "-Ho", "name", brickinfo->path, NULL);
	runner_redir (&runner, STDOUT_FILENO, RUN_PIPE);
	runner_log (&runner, "", GF_LOG_DEBUG, msg);
	ret = runner_start (&runner);
	if (ret == -1) {
		gf_log (this->name, GF_LOG_ERROR, "Failed to get pool name "
			"for device %s", brickinfo->path);
		runner_end (&runner);
		goto out;
	}
	ptr = fgets(zpool, sizeof(zpool),
			runner_chio (&runner, STDOUT_FILENO));
	if (!ptr || !strlen(zpool)) {
		gf_log (this->name, GF_LOG_ERROR, "Failed to get pool name "
			"for snap %s", snapname);
		runner_end (&runner);
		ret = -1;
		goto out;
	}
	runner_end (&runner);

	snprintf (snap, sizeof(snap), "%s@%s_%d", gf_trim(zpool),
			snapname, brickcount);
	snap_device = gf_strdup (snap);
	if (!snap_device) {
		gf_log (this->name, GF_LOG_WARNING, "Cannot copy the "
			"snapshot device name for snapname: %s", snapname);
	}

out:
	return snap_device;
}

static int32_t
glusterd_zfs_snapshot_create (glusterd_brickinfo_t *brickinfo,
                              char *origin_brick_path)
{
	char             msg[NAME_MAX]    = "";
	char             buf[PATH_MAX]    = "";
	char            *ptr              = NULL;
	char            *origin_device    = NULL;
	int              ret              = -1;
	int              len              = 0;
	gf_boolean_t     match            = _gf_false;
	runner_t         runner           = {0,};
	xlator_t        *this             = NULL;
	char            delimiter[]       = "/";
	char            *zpool_name       = NULL;
	char            *zpool_id         = NULL;
	char            *s1               = NULL;
	char            *s2               = NULL;

	this = THIS;
	GF_ASSERT (this);
	GF_ASSERT (brickinfo);
	GF_ASSERT (origin_brick_path);

	s1 = GF_CALLOC(1, 128, gf_gld_mt_char);
	if (!s1) {
		gf_log (this->name, GF_LOG_ERROR,
			"Could not allocate memory for s1");
		goto out;
	}
	s2 = GF_CALLOC(1, 128, gf_gld_mt_char);
	if (!s2) {
		gf_log (this->name, GF_LOG_ERROR,
			"Could not allocate memory for s2");
		goto out;
	}
	strncpy(buf,brickinfo->device_path, sizeof(buf));
	zpool_name = strtok(buf, "@");
	if (!zpool_name) {
		gf_log (this->name, GF_LOG_ERROR,
			"Could not get zfs pool name");
		goto out;
	}
	zpool_id   = strtok(NULL, "@");
	if (!zpool_id) {
		gf_log (this->name, GF_LOG_ERROR,
			"Could not get zfs pool id");
		goto out;
	}
	/* Taking the actual snapshot */
	runinit (&runner);
	snprintf (msg, sizeof (msg), "taking snapshot of the brick %s",
			origin_brick_path);
	runner_add_args (&runner, "zfs", "snapshot", brickinfo->device_path, NULL);
	runner_log (&runner, this->name, GF_LOG_DEBUG, msg);
	ret = runner_run (&runner);
	if (ret) {
		gf_log (this->name, GF_LOG_ERROR, "taking snapshot of the "
			"brick (%s) of device %s failed",
			origin_brick_path, brickinfo->device_path);

		goto end;
	}

	runinit(&runner);
	snprintf (msg, sizeof (msg), "taking clone of the brick %s",
			origin_brick_path);
	sprintf(s1, "%s/%s", zpool_name, zpool_id);
	runner_add_args (&runner, "zfs", "clone", brickinfo->device_path, s1, NULL);
	runner_log (&runner, this->name, GF_LOG_DEBUG, msg);
	ret = runner_run (&runner);
	if (ret) {
		gf_log (this->name, GF_LOG_ERROR, "taking clone of the "
			"brick (%s) of device %s %s failed",
			origin_brick_path, brickinfo->device_path, s1);

		goto end;
	}

	runinit(&runner);
	snprintf (msg, sizeof (msg), "mount clone of the brick %s",
			origin_brick_path);
	sprintf(s2, "mountpoint=%s", brickinfo->path);
	runner_add_args (&runner, "zfs", "set", s2, s1, NULL);
	runner_log (&runner, this->name, GF_LOG_DEBUG, msg);
	ret = runner_run (&runner);
	if (ret) {
		gf_log (this->name, GF_LOG_ERROR, "taking snapshot of the "
			"brick (%s) of device %s %s failed",
			origin_brick_path, s2, s1);
	}

end:
	//runner_end (&runner);
out:
        return ret;
}

static int
glusterd_zfs_snapshot_restore (dict_t *dict, dict_t *rsp_dict,
                        glusterd_volinfo_t *snap_vol,
                        glusterd_volinfo_t *orig_vol,
                        int32_t volcount)
{

	runner_t    runner                             = {0,};
        int         ret                                = -1;
        int32_t                   brickcount           = -1;
	glusterd_brickinfo_t     *brickinfo            = NULL;
	xlator_t                 *this                 = NULL;
	char                      msg[1024]            = {0, };

	this = THIS;

	/*	1. Loop through all bricks in snapvol
		2. Run zfs rollback
		3. if failure , return error
		4. what is rollback process...
	*/

	brickcount = 0;
	list_for_each_entry (brickinfo, &snap_vol->bricks, brick_list) {
		brickcount++;
		runinit (&runner);
		runner_add_args (&runner, "zfs", "rollback", brickinfo->device_path,
					NULL);
		runner_redir (&runner, STDOUT_FILENO, RUN_PIPE);
		snprintf (msg, sizeof (msg), "Start zfs rollback for %s", brickinfo->device_path);
		runner_log (&runner, this->name, GF_LOG_DEBUG, msg);
		ret = runner_start (&runner);
		if (ret == -1) {
			gf_log (this->name, GF_LOG_ERROR, "Failed to rollback "
				"for %s", brickinfo->device_path);
			runner_end (&runner);
			goto out;
		}
	}

	// Delete snapshot object here

	ret = 0;

out:
	return ret;
}

static int
glusterd_zfs_brick_details (dict_t *rsp_dict,
                            glusterd_brickinfo_t *brickinfo, char *volname,
                            char *device, char *key_prefix)
{

	int                     ret             =       -1;
	glusterd_conf_t         *priv           =       NULL;
	runner_t                runner          =       {0,};
	xlator_t                *this           =       NULL;
	char                    msg[PATH_MAX]   =       "";
	char                    buf[PATH_MAX]   =       "";
	char                    *ptr            =       NULL;
	char                    *token          =       NULL;
	char                    key[PATH_MAX]   =       "";
	char                    *value          =       NULL;

	GF_ASSERT (rsp_dict);
	GF_ASSERT (brickinfo);
	GF_ASSERT (volname);
	this = THIS;
	GF_ASSERT (this);
	priv = this->private;
	GF_ASSERT (priv);

	runinit (&runner);
	snprintf (msg, sizeof (msg), "running zfs command, "
			"for getting snap status");

	runner_add_args (&runner, "zfs", "list", "-Ho",
			"used", "-t", "snapshot", brickinfo->device_path, NULL);
	runner_redir (&runner, STDOUT_FILENO, RUN_PIPE);
	runner_log (&runner, "", GF_LOG_DEBUG, msg);
	ret = runner_start (&runner);
	if (ret) {
		gf_log (this->name, GF_LOG_ERROR,
			"Could not perform zfs action");
		goto end;
	}
	do {
		ptr = fgets (buf, sizeof (buf),
			runner_chio (&runner, STDOUT_FILENO));

		if (ptr == NULL)
			break;
		ret = snprintf (key, sizeof (key), "%s.vgname",
				key_prefix);
		if (ret < 0) {
			goto end;
		}

		value = gf_strdup (brickinfo->device_path);
		ret = dict_set_dynstr (rsp_dict, key, value);
		if (ret) {
			gf_log (this->name, GF_LOG_ERROR,
				"Could not save vgname ");
			goto end;
		}

		ret = snprintf (key, sizeof (key), "%s.lvsize",
				key_prefix);
		if (ret < 0) {
			goto end;
		}
		value = gf_strdup (gf_trim(buf));
		ret = dict_set_dynstr (rsp_dict, key, value);
		if (ret) {
			gf_log (this->name, GF_LOG_ERROR,
				"Could not save meta data percent ");
			goto end;
		}
	} while (ptr != NULL);

	ret = 0;
end:
	runner_end (&runner);
	return ret;
}

struct glusterd_snap_ops zfs_snap_ops = {
	.name		= "ZFS",
	/*.probe		= glusterd_zfs_probe,*/
	.details	= glusterd_zfs_brick_details,
	.device		= glusterd_zfs_snapshot_device,
	.create		= glusterd_zfs_snapshot_create,
	/*.missed		= glusterd_zfs_snapshot_missed,*/
	/*.remove		= glusterd_zfs_snapshot_remove,*/
};

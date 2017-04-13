#include <inttypes.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(GF_LINUX_HOST_OS)
#include <mntent.h>
#else
#include "mntent_compat.h"
#endif

#include "glusterd-messages.h"
#include "glusterd-errno.h"

#include "glusterd.h"
#include "glusterd-utils.h"
#include "glusterd-snapshot-utils.h"

#include "dict.h"
#include "run.h"

/* This function will check whether the given device
 * is a ZFS device.
 *
 * @param device        device path
 *
 * @return              _gf_true if zfs else _gf_false
 */
static gf_boolean_t
glusterd_zfs_probe (char *brick_path)
{
	int                   ret               = -1;
	char                 *mnt_pt            = NULL;
	char                  buff[PATH_MAX]    = "";
	struct mntent        *entry             = NULL;
	struct mntent         save_entry        = {0,};
	xlator_t             *this              = NULL;
	gf_boolean_t          is_zfs            = _gf_false;

        this = THIS;
	GF_ASSERT (this);
	GF_ASSERT (brick_path);

        if (!glusterd_is_cmd_available ("/sbin/zfs") ||
            !glusterd_is_cmd_available ("/sbin/zpool")) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_COMMAND_NOT_FOUND, "ZFS commands not found");
                goto out;
        }

	ret = glusterd_get_brick_root (brick_path, &mnt_pt);
	if (ret) {
		gf_msg (this->name, GF_LOG_ERROR, 0,
				GD_MSG_BRICKPATH_ROOT_GET_FAIL,
				"getting the root "
				"of the brick (%s) failed ", brick_path);
		goto out;
	}

	entry = glusterd_get_mnt_entry_info (mnt_pt, buff, sizeof (buff),
			&save_entry);
	if (!entry) {
		gf_msg (this->name, GF_LOG_ERROR, 0,
				GD_MSG_MNTENTRY_GET_FAIL,
				"getting the mount entry for "
				"the brick (%s) failed", brick_path);
		goto out;
	}

	if (0 == strncmp("zfs", entry->mnt_type, 5)) {
		is_zfs = _gf_true;
	}

out:
	if (mnt_pt)
		GF_FREE(mnt_pt);

	return is_zfs;
}

static char *
glusterd_zfs_snapshot_device (char *device, char *snapname,
                              int32_t brickcount)
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
	runner_add_args (&runner, "zfs", "list", "-Ho", "name", device, NULL);
	runner_redir (&runner, STDOUT_FILENO, RUN_PIPE);
	runner_log (&runner, "", GF_LOG_DEBUG, msg);
	ret = runner_start (&runner);
	if (ret == -1) {
		gf_log (this->name, GF_LOG_ERROR, "Failed to get pool name "
			"for device %s", device);
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
	/*char            *ptr              = NULL;*/
	/*char            *origin_device    = NULL;*/
	int              ret              = -1;
	/*int              len              = 0;*/
	/*gf_boolean_t     match            = _gf_false;*/
	runner_t         runner           = {0,};
	xlator_t        *this             = NULL;
	/*char            delimiter[]       = "/";*/
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

#if 0
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
#endif

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
	/*char                    *token          =       NULL;*/
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

int32_t
glusterd_zfs_snapshot_missed (char *volname, char *snapname,
                              glusterd_brickinfo_t *brickinfo,
                              glusterd_snap_op_t *snap_opinfo)
{
        int32_t                      ret              = -1;
        xlator_t                    *this             = NULL;
	char			    *snap_device      = NULL;

	snap_device = glusterd_zfs_snapshot_device (NULL, volname,
			                            snap_opinfo->brick_num - 1);
        if (!snap_device) {
                gf_msg (this->name, GF_LOG_ERROR, ENXIO,
                        GD_MSG_SNAP_DEVICE_NAME_GET_FAIL,
                        "cannot copy the snapshot "
                        "device name (volname: %s, snapname: %s)",
                         volname, snapname);
                ret = -1;
                goto out;
        }
        strncpy (brickinfo->device_path, snap_device,
                 sizeof(brickinfo->device_path));

	ret = glusterd_zfs_snapshot_create (brickinfo, snap_opinfo->brick_path);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_SNAPSHOT_OP_FAILED,
                        "zfs snapshot failed for %s",
                        snap_opinfo->brick_path);
                goto out;
        }

out:
	return ret;
}

int32_t
glusterd_zfs_snapshot_remove (glusterd_volinfo_t *snap_vol,
                                glusterd_brickinfo_t *brickinfo,
                                const char *mount_pt, const char *snap_device)
{
        int                     ret                       = -1;
        xlator_t               *this                      = NULL;
        glusterd_conf_t        *priv                      = NULL;
        runner_t                runner                    = {0,};
        char                    msg[1024]                 = {0, };

        this = THIS;
        GF_ASSERT (this);
        priv = this->private;
        GF_ASSERT (priv);

	runner_add_args (&runner, "/sbin/zfs", "destroy", snap_device, NULL);
        runner_log (&runner, "", GF_LOG_DEBUG, msg);

        ret = runner_run (&runner);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_SNAP_REMOVE_FAIL, "removing snapshot of the "
                        "brick (%s:%s) failed",
                        brickinfo->hostname, brickinfo->path);
		ret = -1;
                goto out;
        }

out:
        return ret;
}

struct glusterd_snap_ops zfs_snap_ops = {
	.name		= "ZFS",
	.probe		= glusterd_zfs_probe,
	.details	= glusterd_zfs_brick_details,
	.device		= glusterd_zfs_snapshot_device,
	.create		= glusterd_zfs_snapshot_create,
	.missed		= glusterd_zfs_snapshot_missed,
	.remove		= glusterd_zfs_snapshot_remove,
};

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

/* This function will check whether the given brick path uses btrfs.
 *
 * @param brick_path   brick path
 *
 * @return             _gf_true if path filesystem is btrfs else _gf_false
 */
gf_boolean_t
glusterd_btrfs_probe (char *brick_path)
{
        int32_t               ret               = -1;
        char                 *mnt_pt            = NULL;
        char                  buff[PATH_MAX]    = "";
        struct mntent        *entry             = NULL;
        struct mntent         save_entry        = {0,};
        xlator_t             *this              = NULL;
	gf_boolean_t          is_btrfs          = _gf_false;

        this = THIS;
        GF_ASSERT (this);
        GF_ASSERT (brick_path);

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

	if (0 == strncmp("btrfs", entry->mnt_type, 5)) {
		is_btrfs = _gf_true;
	}

out:
	if (mnt_pt)
		GF_FREE(mnt_pt);

        return is_btrfs;
}

/* This function is called to get the device path of the btrfs snap.
 * Btrfs poses a uniq problem in that the snapshots are not accessed as a
 * normal device, but instead exist as a directory within the btrfs directory
 * structure. To that end we try to construct a snapshot directory name based
 * on the original volume name.
 */

char *
glusterd_btrfs_snapshot_device (char *brick_path, char *snapname,
                                int32_t brickcount)
{
        char        snap[PATH_MAX]      = "";
        char       *snap_path           = NULL;
        xlator_t   *this                = NULL;

        this = THIS;
        GF_ASSERT (this);

        if (!brick_path) {
                gf_msg (this->name, GF_LOG_ERROR, EINVAL,
                        GD_MSG_INVALID_ENTRY,
                        "brick_path is NULL");
                goto out;
        }
        if (!snapname) {
                gf_msg (this->name, GF_LOG_ERROR, EINVAL,
                        GD_MSG_INVALID_ENTRY,
                        "snapname is NULL");
                goto out;
        }

        snprintf (snap, sizeof(snap), "%s-snapshots/%s_%d", brick_path,
                  snapname, brickcount);
        snap_path = gf_strdup (snap);
        if (!snap_path) {
                gf_msg (this->name, GF_LOG_WARNING, ENOMEM,
                        GD_MSG_NO_MEMORY,
                        "Cannot copy the snapshot device name for snapname: %s",
                        snapname);
        }

out:
        return snap_path;
}

/* Call the 'btrfs' command to take the snapshot of the backend brick
 * filesystem. If this is successful, then call the glusterd_snap_create
 * function to create the snap object for glusterd
*/
int32_t
glusterd_btrfs_snapshot_create (glusterd_brickinfo_t *brickinfo,
                              char *origin_brick_path)
{
        char             msg[NAME_MAX]    = "";
        int              ret              = -1;
        runner_t         runner           = {0,};
        xlator_t        *this             = NULL;

        this = THIS;
        GF_ASSERT (this);
        GF_ASSERT (brickinfo);
        GF_ASSERT (origin_brick_path);

        /* Taking the actual snapshot */
        runinit (&runner);
        snprintf (msg, sizeof (msg), "taking snapshot of the brick %s",
                  origin_brick_path);
        runner_add_args (&runner, "/bin/btrfs", "subvolume", "snapshot",
			          origin_brick_path, brickinfo->device_path,
				  NULL);
        runner_log (&runner, this->name, GF_LOG_DEBUG, msg);
        ret = runner_run (&runner);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_SNAP_CREATION_FAIL,
			"taking snapshot of the brick %s to %s "
			"failed", origin_brick_path, brickinfo->device_path);
        }

        return ret;
}

int32_t
glusterd_btrfs_snapshot_missed (char *volname, char *snapname,
		                glusterd_brickinfo_t *brickinfo,
		                glusterd_snap_op_t *snap_opinfo)
{
        int32_t                      ret              = -1;
        xlator_t                    *this             = NULL;
	char			    *device	      = NULL;
	char			    *snap_device      = NULL;

        /* Fetch the device path */
        device = glusterd_get_brick_mount_device (snap_opinfo->brick_path);
        if (!device) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_BRICK_GET_INFO_FAIL,
                        "Getting device name for the"
                        "brick %s:%s failed", brickinfo->hostname,
                        snap_opinfo->brick_path);
                ret = -1;
                goto out;
        }

	snap_device = glusterd_btrfs_snapshot_device (device, volname,
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


	ret = glusterd_btrfs_snapshot_create (brickinfo, snap_opinfo->brick_path);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_SNAPSHOT_OP_FAILED,
                        "btrfs snapshot failed for %s",
                        snap_opinfo->brick_path);
                goto out;
        }

out:
	return ret;
}


int
glusterd_btrfs_brick_details (dict_t *rsp_dict,
                              glusterd_brickinfo_t *brickinfo, char *volname,
                              char *device, char *key_prefix)
{
        xlator_t                *this           =       NULL;

        GF_ASSERT (rsp_dict);
        GF_ASSERT (brickinfo);
        GF_ASSERT (volname);
        this = THIS;
        GF_ASSERT (this);

	/* FIXME btrfs only supports information such as subvolume usage and
	 * uniq usage if quota's are enabled (though no quotas need to be set).
	 */

        return -1;
}


int
glusterd_btrfs_snapshot_remove (glusterd_volinfo_t *snap_vol,
                                glusterd_brickinfo_t *brickinfo,
                                const char *mount_pt, const char *snap_device)
{
        int                     ret               = -1;
        xlator_t               *this              = NULL;
        glusterd_conf_t        *priv              = NULL;
        runner_t                runner            = {0,};
        char                    msg[1024]         = {0, };
        char                    pidfile[PATH_MAX] = {0, };
        pid_t                   pid               = -1;
        char                   *mnt_pt            = NULL;

        this = THIS;
        GF_ASSERT (this);
        priv = this->private;
        GF_ASSERT (priv);

        if (!brickinfo) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_INVALID_ENTRY, "brickinfo NULL");
                goto out;
        }
        GF_ASSERT (snap_vol);
        GF_ASSERT (mount_pt);
        GF_ASSERT (snap_device);

        GLUSTERD_GET_BRICK_PIDFILE (pidfile, snap_vol, brickinfo, priv);
        if (gf_is_service_running (pidfile, &pid)) {
                int send_attach_req (xlator_t *this, struct rpc_clnt *rpc,
                                     char *path, int op);
                (void) send_attach_req (this, brickinfo->rpc,
                                        brickinfo->path,
                                        GLUSTERD_BRICK_TERMINATE);
                brickinfo->status = GF_BRICK_STOPPED;
        }

	ret = glusterd_snapshot_umount(snap_vol, brickinfo, mount_pt);
	if (ret) {
		goto out;
	}

        runinit (&runner);
        snprintf (msg, sizeof(msg), "remove snapshot of the brick %s:%s, "
                  "device: %s", brickinfo->hostname, brickinfo->path,
                  snap_device);
	/* FIXME Add sanity checks as the command for deleting a snapshot is
	 * the same as deleting a subvolume */
	runner_add_args (&runner, "/bin/btrfs", "subvolume", "delete",
			 snap_device, NULL);
        runner_log (&runner, "", GF_LOG_DEBUG, msg);

        ret = runner_run (&runner);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_SNAP_REMOVE_FAIL, "removing snapshot of the "
                        "brick (%s:%s) of device %s failed",
                        brickinfo->hostname, brickinfo->path, snap_device);
                goto out;
        }

out:
        if (mnt_pt)
                GF_FREE(mnt_pt);

        return ret;
}

struct glusterd_snap_ops btrfs_snap_ops = {
	.name		= "btrfs",
	.probe		= glusterd_btrfs_probe,
	.details	= glusterd_btrfs_brick_details,
	.device		= glusterd_btrfs_snapshot_device,
	.create		= glusterd_btrfs_snapshot_create,
	.missed		= glusterd_btrfs_snapshot_missed,
	.remove		= glusterd_btrfs_snapshot_remove,
	.mount		= glusterd_snapshot_mount,
};

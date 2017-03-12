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

int32_t
glusterd_snapshot_mount (glusterd_brickinfo_t *brickinfo,
		                         char *brick_mount_path);

/* This function will check whether the given brick path uses btrfs.
 *
 * @param brick_path   brick path
 *
 * @return	_gf_true if path filesystem is btrfs else _gf_false
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
 * structure. To that end we are attempting to abuse the returned device_path
 * to act as a way to pass the snapname to glusterd_btrfs_snapshot_create()
 * which will find the real device_path from the origin_brick_path.
 */

char *
glusterd_btrfs_snapshot_device (char *brick_path, char *snapname,
                                int32_t brickcount)
{
	char        snap_dir[PATH_MAX]  = "";
	char       *device_path         = NULL;
        xlator_t   *this                = NULL;

        this = THIS;
        GF_ASSERT (this);
        GF_ASSERT (brick_path);
	GF_ASSERT (snapname);

	snprintf(snap_dir, sizeof(snap_dir),
			"%s_%d", snapname, brickcount);
	device_path= gf_strdup(snap_dir);
	if (!device_path) {
                gf_msg (this->name, GF_LOG_WARNING, ENOMEM,
                        GD_MSG_NO_MEMORY,
                        "Cannot copy the device name for snapname: %s",
			snap_dir);
        }

        return device_path;
}

/* Call the 'btrfs' command to take the snapshot of the backend brick
 * filesystem. If this is successful, then call the glusterd_snap_create
 * function to create the snap object for glusterd
*/
int32_t
glusterd_btrfs_snapshot_create (glusterd_brickinfo_t *brickinfo,
                                char *origin_brick_path)
{
        char             msg[NAME_MAX]             = "";
	char		 btrfs_mnt_path[PATH_MAX]  = "";
	char             btrfs_snap_path[PATH_MAX] = "";
	char            *device_path               = NULL;
	char             subvol[NAME_MAX]          = "";
        int              ret                       = -1;
        runner_t         runner                    = {0,};
        xlator_t        *this                      = NULL;

        this = THIS;
        GF_ASSERT (this);
        GF_ASSERT (brickinfo);
        GF_ASSERT (origin_brick_path);

	/* glusterd_btrfs_snapshot_device() will have stored the subvol in
	 * brickinfo->device_path, so we need to find the right device_path */
	device_path = glusterd_get_brick_mount_device (origin_brick_path);
	if (!device_path) {
		gf_msg (this->name, GF_LOG_ERROR, 0,
			GD_MSG_SNAP_DEVICE_NAME_GET_FAIL,
			"getting the device for brick (%s) failed ",
			origin_brick_path);
		ret = -1;
		goto out;
	}

	/* In the long run we need to find another way to pull this off */
	strcpy(subvol, brickinfo->device_path);
	strcpy(brickinfo->device_path, device_path);

	/* Unlike LVM, we do not need a /dev/ path to perform our snapshot, all
	 * we really need is find the device_path and mount it onto
	 * 'run/gluster/btr/'. */
	snprintf (btrfs_mnt_path, sizeof (btrfs_mnt_path),
			GLUSTERD_VAR_RUN_DIR "/gluster/btrfs/%s",
			subvol);

	/* FIXME we should support some form of btrfs-subvol-prefix option here */
	snprintf (btrfs_snap_path, sizeof (btrfs_snap_path), "%s/@%s",
			btrfs_mnt_path, subvol);

	ret = mkdir_p (btrfs_mnt_path, 0777, _gf_true);
	if (ret) {
		gf_msg (this->name, GF_LOG_ERROR, errno,
			GD_MSG_DIR_OP_FAILED,
			"creating the btrfs mount %s for "
			" brick %s (device: %s) failed",
			btrfs_mnt_path, brickinfo->path,
			brickinfo->device_path );
		goto out;
	}

	/* Clear the mnt_opts so we don't pick up a random subvolume */
	strcpy(brickinfo->mnt_opts, "");

	/* We swapped the device_path out, so this should be viable */
	ret = glusterd_snapshot_mount (brickinfo, btrfs_mnt_path);
	if (ret) {
		gf_msg (this->name, GF_LOG_ERROR, 0,
			GD_MSG_LVM_MOUNT_FAILED,
			"Failed to mount btrfs device (%s) to snapshot "
			" brick (%s).",
			brickinfo->device_path, origin_brick_path);
		goto out;
	}

	/* From here we can perform our snapshot against our origin_brick_path
	 * to 'run/gluster/btr/<subvol>/@<subvol>'. */

        /* Taking the actual snapshot */
        runinit (&runner);
        snprintf (msg, sizeof (msg), "taking snapshot of the brick %s",
                  origin_brick_path);
        runner_add_args (&runner, "/bin/btrfs", "subvolume", "snapshot",
			          origin_brick_path, btrfs_snap_path,
				  NULL);
        runner_log (&runner, this->name, GF_LOG_DEBUG, msg);
        ret = runner_run (&runner);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_SNAP_CREATION_FAIL,
			"taking snapshot of the brick %s to %s "
			"failed", origin_brick_path, brickinfo->device_path);
		ret = glusterd_umount(btrfs_mnt_path);
		if (!ret)
			recursive_rmdir(btrfs_mnt_path);
		ret = -1;
		goto out;
        }

	/* Record our new mnt_opts */
	snprintf (brickinfo->mnt_opts, sizeof (brickinfo->mnt_opts),
			"default,subvol=@%s", subvol);

	/* Cleanup */
	ret = glusterd_umount(btrfs_mnt_path);
	if (!ret)
		recursive_rmdir(btrfs_mnt_path);
out:
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
        int                     ret                       = -1;
        xlator_t               *this                      = NULL;
        glusterd_conf_t        *priv                      = NULL;
        runner_t                runner                    = {0,};
        char                    msg[1024]                 = {0, };
        char                    pidfile[PATH_MAX]         = {0, };
        pid_t                   pid                       = -1;
        char                   *mnt_pt                    = NULL;
	gf_boolean_t		unmount			  = _gf_true;
	int			retry_count		  = 0;
	char		        btrfs_mnt_path[PATH_MAX]  = "";
	char                    btrfs_snap_path[PATH_MAX] = "";
	char		        mnt_opts[NAME_MAX]	  = "";
	char                   *subvol                    = NULL;
	char                   *save_ptr                  = NULL;

        this = THIS;
        GF_ASSERT (this);
        priv = this->private;
        GF_ASSERT (priv);

	/* FIXME: begin copy/paste from glusterd_lvm_snapshot_remove() */

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

        /* Check if the brick is mounted and then try unmounting the brick */
        ret = glusterd_get_brick_root (brickinfo->path, &mnt_pt);
        if (ret) {
                gf_msg (this->name, GF_LOG_WARNING, 0,
                        GD_MSG_BRICK_PATH_UNMOUNTED, "Getting the root "
                        "of the brick for volume %s (snap %s) failed. "
                        "Removing subvolume (%s).", snap_vol->volname,
                         snap_vol->snapshot->snapname, snap_device);
		/* The brick path is already unmounted. Remove the subvolume
		 * only * Need not fail the operation */
                ret = 0;
                unmount = _gf_false;
        }

        if ((unmount == _gf_true) && (strcmp (mnt_pt, mount_pt))) {
                gf_msg (this->name, GF_LOG_WARNING, 0,
                        GD_MSG_BRICK_PATH_UNMOUNTED,
                        "Subvolume is not mounted for brick %s:%s. "
                        "Removing subvolume (%s).", brickinfo->hostname,
                        brickinfo->path, snap_device);
		/* The brick path is already unmounted. Remove the subvolume
		 * only. Need not fail the operation */
		unmount = _gf_false;
        }

        /* umount cannot be done when the brick process is still in the process
           of shutdown, so give three re-tries */
        while ((unmount == _gf_true) && (retry_count < 3)) {
                retry_count++;
                /*umount2 system call doesn't cleanup mtab entry after un-mount.
                  So use external umount command*/
                ret = glusterd_umount(mount_pt);
                if (!ret)
                        break;

                gf_msg_debug (this->name, 0, "umount failed for "
                        "path %s (brick: %s): %s. Retry(%d)", mount_pt,
                        brickinfo->path, strerror (errno), retry_count);

                /*
                 * This used to be one second, but that wasn't long enough
                 * to get past the spurious EPERM errors that prevent some
                 * tests (especially bug-1162462.t) from passing reliably.
                 *
                 * TBD: figure out where that garbage is coming from
                 */
                sleep (3);
        }
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_UNOUNT_FAILED, "umount failed for "
                        "path %s (brick: %s): %s.", mount_pt,
                        brickinfo->path, strerror (errno));
                /*
                 * This is cheating, but necessary until we figure out how to
                 * shut down a brick within a still-living brick daemon so that
                 * random translators aren't keeping the mountpoint alive.
                 *
                 * TBD: figure out a real solution
                 */
                ret = 0;
                goto out;
        }

	/* FIXME end copy/paste from glusterd_lvm_snapshot_remove() */

	/* The brickcount from glusterd_btrfs_snapshot_create() is not
	 * available to us at this point, so we need to parse it from the
	 * brickinfo->mnt_opts */
	strcpy(mnt_opts, brickinfo->mnt_opts);
	subvol = strtok_r(mnt_opts, ",", &save_ptr);
	do {
		if (!subvol)
			break;
		if (0 == strncmp(subvol, "subvol=", 7)) {
			subvol = strtok_r(subvol, "=", &save_ptr);
			subvol = strtok_r(NULL, "=", &save_ptr);
			(void)*subvol++;
			break;
		}
	} while ((subvol = strtok_r(NULL, ",", &save_ptr)));
	if (!subvol) {
		gf_msg (this->name, GF_LOG_ERROR, errno,
			GD_MSG_DIR_OP_FAILED,
			"finding mnt_opts for brick %s failed",
			brickinfo->path);
		ret = -1;
		goto out;
	}

	snprintf (btrfs_mnt_path, sizeof (btrfs_mnt_path),
			GLUSTERD_VAR_RUN_DIR "/gluster/btrfs/%s", subvol);

	snprintf (btrfs_snap_path, sizeof (btrfs_snap_path), "%s/@%s",
			btrfs_mnt_path, subvol);

	ret = mkdir_p (btrfs_mnt_path, 0777, _gf_true);
	if (ret) {
		gf_msg (this->name, GF_LOG_ERROR, errno,
			GD_MSG_DIR_OP_FAILED,
			"creating the btrfs mount %s for "
			" brick %s (subvol: %s) failed",
			btrfs_mnt_path, brickinfo->path, subvol);
		goto out;
	}

	/* We swapped the device_path out, so this should be viable */
	strcpy(mnt_opts, brickinfo->mnt_opts);
	strcpy(brickinfo->mnt_opts, "defaults");
	ret = glusterd_snapshot_mount (brickinfo, btrfs_mnt_path);
	if (ret) {
		gf_msg (this->name, GF_LOG_ERROR, 0,
			GD_MSG_LVM_MOUNT_FAILED,
			"Failed to mount btrfs subvol (%s) to delete "
			"snapshot (%s).", subvol, btrfs_mnt_path);
		goto out;
	}

        runinit (&runner);
        snprintf (msg, sizeof(msg), "remove snapshot of the brick %s:%s, "
                  "subvol: %s", brickinfo->hostname, brickinfo->path,
                  subvol);
	runner_add_args (&runner, "/bin/btrfs", "subvolume", "delete",
			 btrfs_snap_path, NULL);
        runner_log (&runner, "", GF_LOG_DEBUG, msg);

        ret = runner_run (&runner);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_SNAP_REMOVE_FAIL, "removing snapshot of the "
                        "brick (%s:%s) of subvol %s failed",
                        brickinfo->hostname, brickinfo->path, subvol);
		ret = glusterd_umount(btrfs_mnt_path);
		if (!ret)
			recursive_rmdir(btrfs_mnt_path);
		ret = -1;
                goto out;
        }

	ret = glusterd_umount(btrfs_mnt_path);
	if (!ret)
		recursive_rmdir(btrfs_mnt_path);

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

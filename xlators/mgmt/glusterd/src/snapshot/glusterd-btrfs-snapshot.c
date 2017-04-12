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
 * NOTE: the device argument is ignored for BTRFS
 */

char *
glusterd_btrfs_snapshot_device (char *device, char *snapname, int32_t brickcount)
{
	char        snap_dir[PATH_MAX]  = "";
	char       *device_path         = NULL;
        xlator_t   *this                = NULL;

        this = THIS;
        GF_ASSERT (this);
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

static char *
glusterd_btrfs_mount(glusterd_brickinfo_t *brickinfo, char *subvol)
{
	int32_t      ret                         = -1;
	char         btrfs_mnt_path[PATH_MAX]    = "";
	char         mnt_opts[NAME_MAX]          = "";
	char        *mnt_pt                      = "";
	xlator_t    *this                        = NULL;

	this = THIS;
	GF_ASSERT(subvol);
	GF_ASSERT(brickinfo);

	snprintf (btrfs_mnt_path, sizeof (btrfs_mnt_path),
		  GLUSTERD_VAR_RUN_DIR "/gluster/btrfs/%s", subvol);
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

	mnt_pt = gf_strdup(btrfs_mnt_path);

out:
	strcpy(brickinfo->mnt_opts, mnt_opts);

	return mnt_pt;
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
	char             btrfs_snap_path[PATH_MAX] = "";
	char             subvol[NAME_MAX]          = "";
	char            *device_path               = NULL;
	char            *mnt_pt                    = NULL;
	char            *origin_mnt_pt             = NULL;
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

	/* Find the volume from the brick_path */
        ret = glusterd_get_brick_root (origin_brick_path, &origin_mnt_pt);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_BRICKPATH_ROOT_GET_FAIL,
                        "getting the root "
                        "of the brick (%s) failed ", origin_brick_path);
                goto out;
        }

	/* Copy the snapname from the device_path and then fix the device_path.
	 * FIXME In the long run we need to find another way to pull this off
	 */
	strcpy(subvol, brickinfo->device_path);
	strcpy(brickinfo->device_path, device_path);
	mnt_pt = glusterd_btrfs_mount(brickinfo, subvol);
	if (!mnt_pt)
		goto out;

	/* From here we can perform our snapshot against our origin_mnt_pt
	 * to 'run/gluster/btr/<subvol>/@<subvol>'. */
	snprintf (btrfs_snap_path, sizeof(btrfs_snap_path), "%s/@%s",
			mnt_pt, subvol);

        /* Taking the actual snapshot */
        runinit (&runner);
        snprintf (msg, sizeof (msg), "taking snapshot of the brick %s @ %s",
                  origin_brick_path, origin_mnt_pt);
        runner_add_args (&runner, "/bin/btrfs", "subvolume", "snapshot",
			          origin_mnt_pt, btrfs_snap_path,
				  NULL);
        runner_log (&runner, this->name, GF_LOG_DEBUG, msg);
        ret = runner_run (&runner);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_SNAP_CREATION_FAIL,
			"taking snapshot of the brick %s to %s "
			"failed", origin_brick_path, brickinfo->device_path);
		glusterd_umount(mnt_pt, _gf_true);
		ret = -1;
		goto out;
        }

	/* Record our new mnt_opts */
	snprintf (brickinfo->mnt_opts, sizeof (brickinfo->mnt_opts),
			"default,subvol=@%s", subvol);

	ret = glusterd_umount(mnt_pt, _gf_true);
out:
	if (mnt_pt)
		GF_FREE(mnt_pt);

        return ret;
}

int32_t
glusterd_btrfs_snapshot_missed (char *volname, char *snapname,
		                glusterd_brickinfo_t *brickinfo,
		                glusterd_snap_op_t *snap_opinfo)
{
        int32_t                      ret              = -1;
        xlator_t                    *this             = NULL;
	char			    *snap_device      = NULL;

	snap_device = glusterd_btrfs_snapshot_device (NULL, volname,
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


int32_t
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

        return 0;
}


int32_t
glusterd_btrfs_snapshot_remove (glusterd_volinfo_t *snap_vol,
                                glusterd_brickinfo_t *brickinfo,
                                const char *mount_pt, const char *snap_device)
{
        int                     ret                       = -1;
        xlator_t               *this                      = NULL;
        glusterd_conf_t        *priv                      = NULL;
        runner_t                runner                    = {0,};
        char                    msg[1024]                 = {0, };
        char                   *mnt_pt                    = NULL;
	char                    btrfs_snap_path[PATH_MAX] = "";
	char		        mnt_opts[NAME_MAX]	  = "";
	char                   *subvol                    = NULL;
	char                   *save_ptr                  = NULL;

        this = THIS;
        GF_ASSERT (this);
        priv = this->private;
        GF_ASSERT (priv);

	ret = glusterd_snapshot_umount(snap_vol, brickinfo, mount_pt);
	if (ret) {
		goto out;
	}

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

	/* mount the root device so we can manage subvolumes */
	mnt_pt = glusterd_btrfs_mount(brickinfo, subvol);
	if(!mnt_pt)
		goto out;

        runinit (&runner);
	snprintf (btrfs_snap_path, sizeof (btrfs_snap_path), "%s/@%s",
			mnt_pt, subvol);
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
		glusterd_umount(mnt_pt, _gf_true);
		ret = -1;
                goto out;
        }

	ret = glusterd_umount(mnt_pt, _gf_true);

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

/*
   Copyright (c) 2013-2014 Red Hat, Inc. <http://www.redhat.com>
   This file is part of GlusterFS.

   This file is licensed to you under your choice of the GNU Lesser
   General Public License, version 3 or any later version (LGPLv3 or
   later), or the GNU General Public License, version 2 (GPLv2), in all
   cases as published by the Free Software Foundation.
*/
#include <inttypes.h>
#include <sys/types.h>
#include <unistd.h>

#include "glusterd-messages.h"
#include "glusterd-errno.h"

#include "glusterd.h"
#include "glusterd-utils.h"
#include "glusterd-snapshot-utils.h"

#include "dict.h"
#include "run.h"

#include "lvm-defaults.h"
#include "glusterd-lvm-snapshot.h"

/* This function will check whether the given device
 * is a thinly provisioned LV or not.
 *
 * @param device        LV device path
 *
 * @return              _gf_true if LV is thin else _gf_false
 */
gf_boolean_t
glusterd_is_lvm_brick (char *brick_path)
{
        int             ret                     = -1;
        char            msg [1024]              = "";
        char            pool_name [PATH_MAX]    = "";
        char           *ptr                     = NULL;
	char           *device			= NULL;
        xlator_t       *this                    = NULL;
        runner_t        runner                  = {0,};
        gf_boolean_t    is_thin                 = _gf_false;

        this = THIS;

        GF_VALIDATE_OR_GOTO ("glusterd", this, out);
        GF_VALIDATE_OR_GOTO (this->name, brick_path, out);

	device = glusterd_get_brick_mount_device (brick_path);
        if (!device) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_BRICK_GET_INFO_FAIL, "getting device name for "
                        "the brick %s failed", brick_path);
                goto out;
        }

        if (!glusterd_is_cmd_available ("/sbin/lvs")) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_COMMAND_NOT_FOUND, "LVM commands not found");
                ret = -1;
                goto out;
        }

        snprintf (msg, sizeof (msg), "Get thin pool name for device %s",
                  device);

        runinit (&runner);

        runner_add_args (&runner, "/sbin/lvs", "--noheadings", "-o", "pool_lv",
                         device, NULL);
        runner_redir (&runner, STDOUT_FILENO, RUN_PIPE);
        runner_log (&runner, this->name, GF_LOG_DEBUG, msg);

        ret = runner_start (&runner);
        if (ret == -1) {
                gf_msg (this->name, GF_LOG_ERROR, errno,
                        GD_MSG_TPOOL_GET_FAIL, "Failed to get thin pool "
                        "name for device %s", device);
                runner_end (&runner);
                goto out;
        }

        ptr = fgets(pool_name, sizeof(pool_name),
                    runner_chio (&runner, STDOUT_FILENO));
        if (!ptr || !strlen(pool_name)) {
                gf_msg (this->name, GF_LOG_ERROR, errno,
                        GD_MSG_TPOOL_GET_FAIL, "Failed to get pool name "
                        "for device %s", device);
                runner_end (&runner);
                ret = -1;
                goto out;
        }

        runner_end (&runner);

        /* Trim all the whitespaces. */
        ptr = gf_trim (pool_name);

        /* If the LV has thin pool associated with this
         * then it is a thinly provisioned LV else it is
         * regular LV */
        if (0 != ptr [0]) {
                is_thin = _gf_true;
        }

out:
	if (device) {
		GF_FREE(device);
	}

        return is_thin;
}


/* This function is called to get the device path of the snap lvm. Usually
   if /dev/mapper/<group-name>-<lvm-name> is the device for the lvm,
   then the snap device will be /dev/<group-name>/<snapname>.
   This function takes care of building the path for the snap device.
*/

char *
glusterd_lvm_snapshot_device (char *device, char *snapname,
                              int32_t brickcount)
{
        char        snap[PATH_MAX]      = "";
        char        msg[1024]           = "";
        char        volgroup[PATH_MAX]  = "";
        char       *snap_device         = NULL;
        xlator_t   *this                = NULL;
        runner_t    runner              = {0,};
        char       *ptr                 = NULL;
        int         ret                 = -1;

        this = THIS;
        GF_ASSERT (this);
        if (!device) {
                gf_msg (this->name, GF_LOG_ERROR, EINVAL,
                        GD_MSG_INVALID_ENTRY,
                        "device is NULL");
                goto out;
        }
        if (!snapname) {
                gf_msg (this->name, GF_LOG_ERROR, EINVAL,
                        GD_MSG_INVALID_ENTRY,
                        "snapname is NULL");
                goto out;
        }

        runinit (&runner);
        runner_add_args (&runner, "/sbin/lvs", "--noheadings", "-o", "vg_name",
                         device, NULL);
        runner_redir (&runner, STDOUT_FILENO, RUN_PIPE);
        snprintf (msg, sizeof (msg), "Get volume group for device %s", device);
        runner_log (&runner, this->name, GF_LOG_DEBUG, msg);
        ret = runner_start (&runner);
        if (ret == -1) {
                gf_msg (this->name, GF_LOG_ERROR, errno,
                        GD_MSG_VG_GET_FAIL, "Failed to get volume group "
                        "for device %s", device);
                runner_end (&runner);
                goto out;
        }
        ptr = fgets(volgroup, sizeof(volgroup),
                    runner_chio (&runner, STDOUT_FILENO));
        if (!ptr || !strlen(volgroup)) {
                gf_msg (this->name, GF_LOG_ERROR, errno,
                        GD_MSG_VG_GET_FAIL, "Failed to get volume group "
                        "for snap %s", snapname);
                runner_end (&runner);
                ret = -1;
                goto out;
        }
        runner_end (&runner);

        snprintf (snap, sizeof(snap), "/dev/%s/%s_%d", gf_trim(volgroup),
                  snapname, brickcount);
        snap_device = gf_strdup (snap);
        if (!snap_device) {
                gf_msg (this->name, GF_LOG_WARNING, ENOMEM,
                        GD_MSG_NO_MEMORY,
                        "Cannot copy the snapshot device name for snapname: %s",
                        snapname);
        }

out:
        return snap_device;
}

/* This function actually calls the command (or the API) for taking the
   snapshot of the backend brick filesystem. If this is successful,
   then call the glusterd_snap_create function to create the snap object
   for glusterd
*/
int32_t
glusterd_lvm_snapshot_create (glusterd_brickinfo_t *brickinfo,
                              char *origin_brick_path)
{
        char             msg[NAME_MAX]    = "";
        char             buf[PATH_MAX]    = "";
        char            *ptr              = NULL;
        char            *origin_device    = NULL;
        int              ret              = -1;
        gf_boolean_t     match            = _gf_false;
        runner_t         runner           = {0,};
        xlator_t        *this             = NULL;

        this = THIS;
        GF_ASSERT (this);
        GF_ASSERT (brickinfo);
        GF_ASSERT (origin_brick_path);

        origin_device = glusterd_get_brick_mount_device (origin_brick_path);
        if (!origin_device) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_BRICK_GET_INFO_FAIL, "getting device name for "
                        "the brick %s failed", origin_brick_path);
                goto out;
        }

        /* Figuring out if setactivationskip flag is supported or not */
        runinit (&runner);
        snprintf (msg, sizeof (msg), "running lvcreate help");
        runner_add_args (&runner, LVM_CREATE, "--help", NULL);
        runner_log (&runner, "", GF_LOG_DEBUG, msg);
        runner_redir (&runner, STDOUT_FILENO, RUN_PIPE);
        ret = runner_start (&runner);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, errno,
                        GD_MSG_LVCREATE_FAIL,
                        "Failed to run lvcreate help");
                runner_end (&runner);
                goto out;
        }

        /* Looking for setactivationskip in lvcreate --help */
        do {
                ptr = fgets(buf, sizeof(buf),
                            runner_chio (&runner, STDOUT_FILENO));
                if (ptr) {
                        if (strstr(buf, "setactivationskip")) {
                                match = _gf_true;
                                break;
                        }
                }
        } while (ptr != NULL);
        runner_end (&runner);

        /* Taking the actual snapshot */
        runinit (&runner);
        snprintf (msg, sizeof (msg), "taking snapshot of the brick %s",
                  origin_brick_path);
        if (match == _gf_true)
                runner_add_args (&runner, LVM_CREATE, "-s", origin_device,
                                 "--setactivationskip", "n", "--name",
                                 brickinfo->device_path, NULL);
        else
                runner_add_args (&runner, LVM_CREATE, "-s", origin_device,
                                 "--name", brickinfo->device_path, NULL);
        runner_log (&runner, this->name, GF_LOG_DEBUG, msg);
        ret = runner_run (&runner);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_SNAP_CREATION_FAIL, "taking snapshot of the "
                        "brick (%s) of device %s failed",
                        origin_brick_path, origin_device);
        }

out:
        if (origin_device)
                GF_FREE (origin_device);

        return ret;
}

int
glusterd_lvm_brick_details (dict_t *rsp_dict,
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

        device = glusterd_get_brick_mount_device (brickinfo->path);
        if (!device) {
                gf_msg (this->name, GF_LOG_ERROR, 0, GD_MSG_BRICK_GET_INFO_FAIL,
                        "Getting device name for "
                        "the brick %s:%s failed", brickinfo->hostname,
                         brickinfo->path);
                goto out;
        }
        runinit (&runner);
        snprintf (msg, sizeof (msg), "running lvs command, "
                  "for getting snap status");
        /* Using lvs command fetch the Volume Group name,
         * Percentage of data filled and Logical Volume size
         *
         * "-o" argument is used to get the desired information,
         * example : "lvs /dev/VolGroup/thin_vol -o vgname,lv_size",
         * will get us Volume Group name and Logical Volume size.
         *
         * Here separator used is ":",
         * for the above given command with separator ":",
         * The output will be "vgname:lvsize"
         */
        runner_add_args (&runner, LVS, device, "--noheading", "-o",
                         "vg_name,data_percent,lv_size",
                         "--separator", ":", NULL);
        runner_redir (&runner, STDOUT_FILENO, RUN_PIPE);
        runner_log (&runner, "", GF_LOG_DEBUG, msg);
        ret = runner_start (&runner);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, errno,
                        GD_MSG_LVS_FAIL,
                        "Could not perform lvs action");
                goto end;
        }
        do {
                ptr = fgets (buf, sizeof (buf),
                             runner_chio (&runner, STDOUT_FILENO));

                if (ptr == NULL)
                        break;
                token = strtok (buf, ":");
                if (token != NULL) {
                        while (token && token[0] == ' ')
                                token++;
                        if (!token) {
                                ret = -1;
                                gf_msg (this->name, GF_LOG_ERROR, EINVAL,
                                        GD_MSG_INVALID_ENTRY,
                                        "Invalid vg entry");
                                goto end;
                        }
                        value = gf_strdup (token);
                        if (!value) {
                                ret = -1;
                                goto end;
                        }
                        ret = snprintf (key, sizeof (key), "%s.vgname",
                                        key_prefix);
                        if (ret < 0) {
                                goto end;
                        }

                        ret = dict_set_dynstr (rsp_dict, key, value);
                        if (ret) {
                                gf_msg (this->name, GF_LOG_ERROR, 0,
                                        GD_MSG_DICT_SET_FAILED,
                                        "Could not save vgname ");
                                goto end;
                        }
                }

                token = strtok (NULL, ":");
                if (token != NULL) {
                        value = gf_strdup (token);
                        if (!value) {
                                ret = -1;
                                goto end;
                        }
                        ret = snprintf (key, sizeof (key), "%s.data",
                                        key_prefix);
                        if (ret < 0) {
                                goto end;
                        }

                        ret = dict_set_dynstr (rsp_dict, key, value);
                        if (ret) {
                                gf_msg (this->name, GF_LOG_ERROR, 0,
                                        GD_MSG_DICT_SET_FAILED,
                                        "Could not save data percent ");
                                goto end;
                        }
                }
                token = strtok (NULL, ":");
                if (token != NULL) {
                        value = gf_strdup (token);
                        if (!value) {
                                ret = -1;
                                goto end;
                        }
                        ret = snprintf (key, sizeof (key), "%s.lvsize",
                                        key_prefix);
                        if (ret < 0) {
                                goto end;
                        }

                        ret = dict_set_dynstr (rsp_dict, key, value);
                        if (ret) {
                                gf_msg (this->name, GF_LOG_ERROR, 0,
                                        GD_MSG_DICT_SET_FAILED,
                                        "Could not save meta data percent ");
                                goto end;
                        }
                }

        } while (ptr != NULL);

        ret = 0;

end:
        runner_end (&runner);

out:
        if (ret && value) {
                GF_FREE (value);
        }

        if (device)
                GF_FREE (device);

        return ret;
}

int
glusterd_lvm_snapshot_remove (glusterd_volinfo_t *snap_vol,
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
        int                     retry_count       = 0;
        char                   *mnt_pt            = NULL;
        gf_boolean_t            unmount           = _gf_true;

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

        /* Check if the brick is mounted and then try unmounting the brick */
        ret = glusterd_get_brick_root (brickinfo->path, &mnt_pt);
        if (ret) {
                gf_msg (this->name, GF_LOG_WARNING, 0,
                        GD_MSG_BRICK_PATH_UNMOUNTED, "Getting the root "
                        "of the brick for volume %s (snap %s) failed. "
                        "Removing lv (%s).", snap_vol->volname,
                         snap_vol->snapshot->snapname, snap_device);
                /* The brick path is already unmounted. Remove the lv only *
                 * Need not fail the operation */
                ret = 0;
                unmount = _gf_false;
        }

        if ((unmount == _gf_true) && (strcmp (mnt_pt, mount_pt))) {
                gf_msg (this->name, GF_LOG_WARNING, 0,
                        GD_MSG_BRICK_PATH_UNMOUNTED,
                        "Lvm is not mounted for brick %s:%s. "
                        "Removing lv (%s).", brickinfo->hostname,
                        brickinfo->path, snap_device);
                /* The brick path is already unmounted. Remove the lv only *
                 * Need not fail the operation */
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

        runinit (&runner);
        snprintf (msg, sizeof(msg), "remove snapshot of the brick %s:%s, "
                  "device: %s", brickinfo->hostname, brickinfo->path,
                  snap_device);
        runner_add_args (&runner, LVM_REMOVE, "-f", snap_device, NULL);
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

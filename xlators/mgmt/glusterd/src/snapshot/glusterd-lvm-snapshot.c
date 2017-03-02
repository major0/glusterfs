#include <stdio.h>

/* This function is called to get the device path of the snap lvm. Usually
   if /dev/mapper/<group-name>-<lvm-name> is the device for the lvm,
   then the snap device will be /dev/<group-name>/<snapname>.
   This function takes care of building the path for the snap device.
*/

char *
glusterd_build_snap_device_path (char *device, char *snapname,
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

/* This function will restore a snapshot volumes
 *
 * @param dict          dictionary containing snapshot restore request
 * @param op_errstr     In case of any failure error message will be returned
 *                      in this variable
 * @return              Negative value on Failure and 0 in success
 */
int
glusterd_snapshot_restore (dict_t *dict, char **op_errstr, dict_t *rsp_dict)
{
        int                     ret             = -1;
        int32_t                 volcount        = -1;
        char                    *snapname       = NULL;
        xlator_t                *this           = NULL;
        glusterd_volinfo_t      *snap_volinfo   = NULL;
        glusterd_volinfo_t      *tmp            = NULL;
        glusterd_volinfo_t      *parent_volinfo = NULL;
        glusterd_snap_t         *snap           = NULL;
        glusterd_conf_t         *priv           = NULL;

        this = THIS;

        GF_ASSERT (this);
        GF_ASSERT (dict);
        GF_ASSERT (op_errstr);
        GF_ASSERT (rsp_dict);

        priv = this->private;
        GF_ASSERT (priv);

        ret = dict_get_str (dict, "snapname", &snapname);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_DICT_GET_FAILED,
                        "Failed to get snap name");
                goto out;
        }

        snap = glusterd_find_snap_by_name (snapname);
        if (NULL == snap) {
                ret = gf_asprintf (op_errstr, "Snapshot (%s) does not exist",
                                   snapname);
                if (ret < 0) {
                        goto out;
                }
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        GD_MSG_SNAP_NOT_FOUND,
                        "%s", *op_errstr);
                ret = -1;
                goto out;
        }

        volcount = 0;
        cds_list_for_each_entry_safe (snap_volinfo, tmp, &snap->volumes,
                                      vol_list) {
                volcount++;
                ret = glusterd_volinfo_find (snap_volinfo->parent_volname,
                                             &parent_volinfo);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, EINVAL,
                                GD_MSG_VOL_NOT_FOUND,
                                "Could not get volinfo of %s",
                                snap_volinfo->parent_volname);
                        goto out;
                }

                ret = dict_set_dynstr_with_alloc (rsp_dict, "snapuuid",
                                                  uuid_utoa (snap->snap_id));
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                GD_MSG_DICT_SET_FAILED,
                                "Failed to set snap "
                                "uuid in response dictionary for %s snapshot",
                                snap->snapname);
                        goto out;
                }


                ret = dict_set_dynstr_with_alloc (rsp_dict, "volname",
                                                  snap_volinfo->parent_volname);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                GD_MSG_DICT_SET_FAILED,
                                "Failed to set snap "
                                "uuid in response dictionary for %s snapshot",
                                snap->snapname);
                        goto out;
                }

                ret = dict_set_dynstr_with_alloc (rsp_dict, "volid",
                                        uuid_utoa (parent_volinfo->volume_id));
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                GD_MSG_DICT_SET_FAILED,
                                "Failed to set snap "
                                "uuid in response dictionary for %s snapshot",
                                snap->snapname);
                        goto out;
                }

                if (is_origin_glusterd (dict) == _gf_true) {
                        /* From origin glusterd check if      *
                         * any peers with snap bricks is down */
                        ret = glusterd_find_missed_snap
                                               (rsp_dict, snap_volinfo,
                                                &priv->peers,
                                                GF_SNAP_OPTION_TYPE_RESTORE);
                        if (ret) {
                                gf_msg (this->name, GF_LOG_ERROR, 0,
                                        GD_MSG_MISSED_SNAP_GET_FAIL,
                                        "Failed to find missed snap restores");
                                goto out;
                        }
                }

                ret = gd_restore_snap_volume (dict, rsp_dict, parent_volinfo,
                                              snap_volinfo, volcount);
                if (ret) {
                        /* No need to update op_errstr because it is assumed
                         * that the called function will do that in case of
                         * failure.
                         */
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                GD_MSG_SNAP_RESTORE_FAIL, "Failed to restore "
                                "snap for %s", snapname);
                        goto out;
                }

                /* Detach the volinfo from priv->volumes, so that no new
                 * command can ref it any more and then unref it.
                 */
                cds_list_del_init (&parent_volinfo->vol_list);
                glusterd_volinfo_unref (parent_volinfo);

        }

        ret = 0;

        /* TODO: Need to check if we need to delete the snap after the
         * operation is successful or not. Also need to persist the state
         * of restore operation in the store.
         */
out:
        return ret;
}

/* This function actually calls the command (or the API) for taking the
   snapshot of the backend brick filesystem. If this is successful,
   then call the glusterd_snap_create function to create the snap object
   for glusterd
*/
int32_t
glusterd_take_lvm_snapshot (glusterd_brickinfo_t *brickinfo,
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
glusterd_get_brick_lvm_details (dict_t *rsp_dict,
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

/* This function is called if snapshot restore operation
 * is successful. It will cleanup the backup files created
 * during the restore operation.
 *
 * @param rsp_dict Response dictionary
 * @param volinfo  volinfo of the volume which is being restored
 * @param snap     snap object
 *
 * @return 0 on success or -1 on failure
 */
int
glusterd_snapshot_restore_cleanup (dict_t *rsp_dict,
                                   char *volname,
                                   glusterd_snap_t *snap)
{
        int                     ret                     = -1;
        xlator_t               *this                    = NULL;

        this = THIS;
        GF_ASSERT (this);

        GF_ASSERT (rsp_dict);
        GF_ASSERT (volname);
        GF_ASSERT (snap);

        /* Now delete the snap entry. */
        ret = glusterd_snap_remove (rsp_dict, snap, _gf_false, _gf_true,
                                    _gf_false);
        if (ret) {
                gf_msg (this->name, GF_LOG_WARNING, 0,
                        GD_MSG_SNAP_REMOVE_FAIL, "Failed to delete "
                        "snap %s", snap->snapname);
                goto out;
        }

        /* Delete the backup copy of volume folder */
        ret = glusterd_remove_trashpath(volname);
        if (ret) {
                gf_msg (this->name, GF_LOG_ERROR, errno,
                        GD_MSG_DIR_OP_FAILED, "Failed to remove "
                        "backup dir");
                goto out;
        }

        ret = 0;
out:
        return ret;
}

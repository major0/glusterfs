/*
   Copyright (c) 2013-2014 Red Hat, Inc. <http://www.redhat.com>
   This file is part of GlusterFS.

   This file is licensed to you under your choice of the GNU Lesser
   General Public License, version 3 or any later version (LGPLv3 or
   later), or the GNU General Public License, version 2 (GPLv2), in all
   cases as published by the Free Software Foundation.
*/

#ifndef GLUSTERD_LVM_SNAPSHOT_H
#define GLUSTERD_LVM_SNAPSHOT_H

gf_boolean_t
glusterd_is_lvm_cmd_available (char *lvm_cmd);

gf_boolean_t
glusterd_is_lvm_brick (char *device, uint32_t *op_errno);

int
glusterd_lvm_brick_details (dict_t *rsp_dict,
                            glusterd_brickinfo_t *brickinfo, char *volname,
                            char *device, char *key_prefix);

char *
glusterd_lvm_snapshot_device (char *device, char *snapname,
                              int32_t brickcount);

int32_t
glusterd_lvm_snapshot_create (glusterd_brickinfo_t *brickinfo,
                              char *origin_brick_path);

int32_t
glusterd_lvm_snapshot_mount (glusterd_brickinfo_t *brickinfo,
                             char *brick_mount_path);

int32_t
glusterd_lvm_snapshot_remove (dict_t *rsp_dict, glusterd_volinfo_t *snap_vol);

int
glusterd_lvm_snapshot_restore (dict_t *dict, char **op_errstr, dict_t *rsp_dict);

int
glusterd_lvm_snapshot_restore_cleanup (dict_t *rsp_dict,
                                       char *volname,
                                       glusterd_snap_t *snap);

#endif /* GLUSTERD_LVM_SNAPSHOT_H */

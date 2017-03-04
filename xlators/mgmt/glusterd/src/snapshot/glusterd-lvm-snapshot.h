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

char *
glusterd_lvm_snap_device_path (char *device, char *snapname,
                                 int32_t brickcount);

int
glusterd_lvm_snapshot_restore (dict_t *dict, char **op_errstr, dict_t *rsp_dict);

int32_t
glusterd_lvm_take_snapshot (glusterd_brickinfo_t *brickinfo,
                            char *origin_brick_path);
int
glusterd_lvm_get_brick_details (dict_t *rsp_dict,
                               glusterd_brickinfo_t *brickinfo, char *volname,
                                char *device, char *key_prefix);
int
glusterd_lvm_snapshot_restore_cleanup (dict_t *rsp_dict,
                                   char *volname,
                                   glusterd_snap_t *snap);

int32_t
glusterd_lvm_snapshot_remove (dict_t *rsp_dict, glusterd_volinfo_t *snap_vol);

#endif /* GLUSTERD_LVM_SNAPSHOT_H */

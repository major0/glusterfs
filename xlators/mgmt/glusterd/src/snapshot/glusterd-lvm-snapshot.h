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
glusterd_is_lvm_brick (char *brick_path);

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
glusterd_lvm_snapshot_missed (char *volname, char *snapname,
		              glusterd_brickinfo_t *brickinfo,
		              glusterd_snap_op_t *snap_opinfo);

int32_t
glusterd_lvm_snapshot_remove (glusterd_volinfo_t *snap_vol,
                              glusterd_brickinfo_t *brickinfo,
                              const char *mount_pt, const char *snap_device);

#endif /* GLUSTERD_LVM_SNAPSHOT_H */

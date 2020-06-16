// tablegroup.h
//	  Commands to manipulate table groups
// src/include/commands/tablespace.h
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

#ifndef TABLEGROUP_H
#define TABLEGROUP_H

#include "access/xlogreader.h"
#include "catalog/objectaddress.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"

/* XLOG stuff */
#define XLOG_TBLGRP_CREATE		0x00
#define XLOG_TBLGRP_DROP		0x10

typedef struct xl_tblspc_create_rec
{
	Oid			ts_id;
	char		ts_path[FLEXIBLE_ARRAY_MEMBER]; /* null-terminated string */
} xl_tblspc_create_rec;

typedef struct xl_tblspc_drop_rec
{
	Oid			ts_id;
} xl_tblspc_drop_rec;

typedef struct TableSpaceOpts
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	float8		random_page_cost;
	float8		seq_page_cost;
	int			effective_io_concurrency;
} TableSpaceOpts;

extern Oid	CreateTableSpace(CreateTableSpaceStmt *stmt);
extern void DropTableSpace(DropTableSpaceStmt *stmt);
extern ObjectAddress RenameTableSpace(const char *oldname, const char *newname);
extern Oid	AlterTableSpaceOptions(AlterTableSpaceOptionsStmt *stmt);

extern void TablespaceCreateDbspace(Oid spcNode, Oid dbNode, bool isRedo);

extern Oid	GetDefaultTablespace(char relpersistence);

extern void PrepareTempTablespaces(void);

extern Oid	get_tablespace_oid(const char *tablespacename, bool missing_ok);
extern char *get_tablespace_name(Oid spc_oid);

extern bool directory_is_empty(const char *path);
extern void remove_tablespace_symlink(const char *linkloc);

extern void tblspc_redo(XLogReaderState *rptr);
extern void tblspc_desc(StringInfo buf, XLogReaderState *rptr);
extern const char *tblspc_identify(uint8 info);

#endif							/* TABLESPACE_H */

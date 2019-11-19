/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "seclabel.h"
#include <miscadmin.h>

#define SECLABEL_DIST_TAG_SEPARATOR ":"
#define SECLABEL_DIST_TAG_SEPARATOR_LEN 1
#define SECLABEL_DIST_TAG "dist_uuid"

void
ts_seclabel_set_dist_uuid(Oid dbid, Datum dist_uuid)
{
	ObjectAddress dbobj;
	Datum uuid_string = DirectFunctionCall1(uuid_out, dist_uuid);
	const char *label =
		psprintf(SECLABEL_DIST_TAG SECLABEL_DIST_TAG_SEPARATOR "%s", DatumGetCString(uuid_string));

	ObjectAddressSet(dbobj, DatabaseRelationId, dbid);
	SetSecurityLabel(&dbobj, SECLABEL_DIST_PROVIDER, label);
}

bool
ts_seclabel_get_dist_uuid(Oid dbid, char **uuid)
{
	ObjectAddress dbobj;
	char *uuid_ptr;
	char *label;

	*uuid = NULL;
	ObjectAddressSet(dbobj, DatabaseRelationId, dbid);
	label = GetSecurityLabel(&dbobj, SECLABEL_DIST_PROVIDER);
	if (label == NULL)
		return false;

	uuid_ptr = strchr(label, ':');
	if (uuid_ptr == NULL)
		return false;
	++uuid_ptr;
	*uuid = uuid_ptr;
	return true;
}

static void
seclabel_provider(const ObjectAddress *object, const char *seclabel)
{
}

void
ts_seclabel_init(void)
{
	register_label_provider(SECLABEL_DIST_PROVIDER, seclabel_provider);
}

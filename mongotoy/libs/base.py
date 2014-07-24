# coding: utf8

import copy


def generate_field(parent, field):
    return "%s.%s" % (parent, field)


def get_collection_name(name):
    collection_name = copy.copy(name)
    collection_name = collection_name[0].lower() + collection_name[1:]
    for c in name:
        if c.isupper():
            collection_name = collection_name.replace(c, "_" + c.lower())
    return collection_name

# coding: utf8

import copy


def generate_field(parent, field):
    return "%s.%s" % (parent, field)


def get_collection_name(model_name):
    collection_name = copy.copy(model_name)
    collection_name = collection_name[0].lower() + collection_name[1:]
    for character in model_name:
        if character.isupper():
            collection_name = collection_name.replace(
                character, "_" + character.lower()
            )
    return collection_name

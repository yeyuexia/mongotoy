# coding: utf8

import copy

from .session import get_session
from .util import generate_field
from .operators import Set, update_operator_compiler
from .consts import (
    QUERY_FIND, ASCENDING, DESCENDING,
    DELETE, UPDATE
)


class BaseQuery(object):
    """
    base Query object.
    """

    def __init__(self, model, **kwargs):
        self.parent = None
        self.model = model
        self.commands = dict()
        self.commands["spec"] = kwargs.get("spec")
        self.commands["sort"] = kwargs.get("sort")
        self.commands["fields"] = kwargs.get("fields")
        try:
            self.commands["skip"] = int(kwargs.get("skip", 0))
        except ValueError:
            raise ValueError("skip must be int")

    def __repr__(self):
        collection, command, query = self._compile_context()
        return "%s.%s(%s)" % (
            collection, command,
            ", ".join(
                ["%s=%s" % (key, value) for key, value in query.iteritems()]
            )
        )

    def get_commands(self):
        commands = copy.deepcopy(self.commands)
        if self.parent:
            for key in ("spec", "fields", "sort"):
                values = commands[key]
                if values:
                    if isinstance(values, (list, tuple)):
                        new_values = []
                        for v in values:
                            if isinstance(v, tuple):
                                new_v = [generate_field(self.parent, v[0])]
                                new_v.extend(list(v[1:]))
                                new_values.append(new_v)
                            else:
                                new_values.append(
                                    generate_field(self.parent, v)
                                )
                        commands[key] = new_values
                    elif isinstance(values, dict):
                        new_values = dict()
                        for k, v in values.iteritems():
                            k = generate_field(self.parent, k)
                            new_values[k] = v
                        commands[key] = new_values
                    else:
                        commands[key] = generate_field(
                            self.parent, values
                        )
        else:
            commands["timeout"] = False
        return commands

    def _compile_context(self):
        raise NotImplementedError()

    def execute_context(self, context):
        raise NotImplementedError()

    def sort(self, **kwargs):
        """
        Sorts this cursor’s results.

        Pass a field name and a direction, either ASCENDING or DESCENDING:

        query().sort(field_one=ASCENDING, field_two=DESCENDING, ...)
        """
        for key, value in kwargs.iteritems():
            self.model.assert_vaild_field(key)
            if value not in (DESCENDING, ASCENDING):
                raise ValueError("sort, invaild key")
        self.commands["sort"] = kwargs
        return self

    def skip(self, num):
        """
        Skips the first skip results of this cursor.

        Raises TypeError if skip is not an integer.
        Raises ValueError if skip is less than 0.
        Raises InvalidOperation if this Cursor has already been used.
        The last skip applied to this cursor takes precedence.

        eg:
            query().skip(number)
        """
        try:
            self.commands["skip"] = int(num)
        except ValueError:
            raise ValueError("skip must be int")

    def filters(self, fields):
        """
        """
        for field in fields:
            self.model.assert_vaild_field(field)
        self.commands["fields"] = fields


class Query(BaseQuery):
    def __init__(self, model, **kwargs):
        super(Query, self).__init__(model, **kwargs)
        self.cursor = None
        self.commands["limit"] = kwargs.get("limit", 0)

    def __getitem__(self, item):
        pass

    def __iter__(self):
        if self.cursor is None:
            context = self._compile_context()
            self.execute_context(context)
        return self

    def next(self):
        if self.cursor is None:
            context = self._compile_context()
            self.execute_context(context)
        record = next(self.cursor)
        return self.model(__persistence__=True, **record)

    def _compile_context(self, operation=QUERY_FIND):
        collection = self.model.__collectionname__
        return (collection, operation, self.get_commands())

    def execute_context(self, context):
        self.cursor = get_session().execute(*context)

    def all(self):
        """
        Return a list of all object
        """
        return list(self)

    def limit(self, capicity):
        """
        query().limit(count)
        """
        assert isinstance(capicity, int), "limit access int argument"
        self.commands["limit"] = capicity
        return self

    def update(self, if_not_create=False, multi=True, *args, **kwargs):
        """
        update model
        """
        args = list(args)
        if kwargs and not any(isinstance(x, Set) for x in args):
            args.append(Set(kwargs))
        update = Update(self.model,
                        spec=self.get_commands()["spec"],
                        document=args,
                        multi=multi,
                        upsert=if_not_create)
        return get_session().execute(*update.compile_context())

    def delete(self):
        collection = self.model.__collectionname__
        get_session().execute(
            collection, DELETE, dict(spec_or_id=self.get_commands()["spec"])
        )

    def count(self):
        """
        get the records count in this query operator
        """
        if self.cursor is None:
            context = self._compile_context()
            self.execute_context(context)
        return self.cursor.count()


class QueryOne(BaseQuery):

    def _compile_context(self):
        collection = self.model.__collectionname__
        return (collection, QUERY_FIND, self.get_commands())

    def execute_context(self, context):
        for result in get_session().execute(*context).limit(-1):
            return self.model(__persistence__=True, **result)

    def __call__(self):
        return self.execute_context(self._compile_context())


class Update(object):
    """
    Update object, wrap the params of find method in pymongo
    params:
        model:
        query:
        update:
        multi:
        upsert:
    """
    def __init__(self, model, spec, document, multi, upsert):
        self.model = model
        self.spec = spec
        self.document = document
        self.multi = multi
        self.upsert = upsert

    def _generate_commands(self):

        commands = dict()
        for expression in self.document:
            commands.update(update_operator_compiler(self.model, expression))
        return commands

    def get_commands(self):
        return dict(
            spec=self.spec,
            document=self._generate_commands(),
            multi=self.multi,
            upsert=self.upsert
        )

    def compile_context(self):
        collection = self.model.__collectionname__
        return (collection, UPDATE, self.get_commands())

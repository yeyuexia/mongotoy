## Welcome to Mongotoy

Mongotoy is a simple Python Mongo toolkit and Object Relational Mapper that makes mongo collections more clear.

## Usage

* Declare collection.

```
from mongotoy.libs import Model, SubModel, Field
class TestModel(Model):

    class ModelField(SubModel):
        field1 = Field(float, 0)
        field2 = Field(set, set())

    field1 = Field(int, 0)
    field2 = Field(dict, {})
    field3 = Field(ModelField)

```

* Build connection with Mongo DB with host and port, and register TestModel

```
from mongotoy.libs.session import create_session

collection_mapper = {"db1": "TestModel"}

create_session(host, port=port, collection_mapper=collection_mapper)
```

collection_mapper is a dict and its key is db name and key is the model name db, you can alse use

```
from mongotoy.libs.session import loads_db_mapper
loads_db_mapper(collection_mapper)
```

to load or reload mapper.

* Create record

```
record = TestModel(field1=10, field2={1: 2},
                   field3=TestModel.ModelField(field1=3.0, field2=set([1,2,3])))
```

* Save record

```
flush()
```
Notes: all change need invoke flush method when you need to submit it to mongo server.

* Query

```
cursor = TestModel.query(field1=1)
for model_instance in cursor:
    pass
```

* Get model instance

```
model = TestModel.get_by(field1=1)
```

* Get model instance by id

```
model = TestModel.get(_id)
```

* Convert to dict

```
record = model.to_dict()
```

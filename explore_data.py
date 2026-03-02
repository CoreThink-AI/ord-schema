"""
The `ord-data` repository contains the Open Reaction Database (ORD) in Google's Protobuf binary
format, which is stored in the [`data`](data) directory. Currently, all the data are stored in e.g.
*.pb.gz format (compressed Protobuf binary files) for the sake of efficiency. The user can convert
the data into human readable text format, *.pb.txt.
"""
from google.protobuf.json_format import MessageToJson
import json
from ord_schema.message_helpers import load_message, write_message
from ord_schema.proto import dataset_pb2
from ord_schema.message_helpers import load_message, write_message
from ord_schema.proto import dataset_pb2
from pathlib import Path
import random

provenances = [tuple('ismined doi patent'.split())]
samples = []
print('\t'.join(provenances[-1]))
count = 0
for path in Path('data').glob('**/*.pb.gz'):
    print(path)
    dataset = load_message(str(path), dataset_pb2.Dataset)
    # write_message(dataset, str(path.with_suffix('.pbtxt')))
    i = random.randint(0, len(dataset.reactions) - 1)
    rxn = dataset.reactions[i]
    samples.append(json.loads(
        MessageToJson(
            message=rxn,
            including_default_value_fields=False,
            preserving_proto_field_name=True,
#            indent=0,
            sort_keys=False,
            use_integers_for_enums=False,
            descriptor_pool=None,
            float_precision=None,
            ensure_ascii=True,
        )
    ))
    count += len(dataset.reactions)
    provenances.append(tuple([str(samples[-1]['provenance'].get(c, '')) for c in 'is_mined doi patent'.split()]))
    with open('reaction_samples.jsonlines', 'at') as fout:
        fout.write(json.dumps(samples[-1]) + '\n')

    print(f".reactions[{i}] (1 out of {len(dataset.reactions)}) in {path} was converted JSON:")
    print(json.dumps(samples[-1], indent=4))
    print('\t'.join(provenances[-1]))


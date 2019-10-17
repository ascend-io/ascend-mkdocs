import argparse
from ascend.client import Client
import configparser
from datetime import datetime
import json
import os
from urllib.error import HTTPError

parser = argparse.ArgumentParser()
parser.add_argument('--host', nargs=1, help='The hostname of the Ascend environment to connect to', default=['trial.ascend.io'])
parser.add_argument('--profile', nargs=1, help='The profile used to locate Ascend credentials in ~/.ascend/credentials', default=['ascend'])
parser.add_argument('ds_id', nargs='*', help='IDs of Data Services to be processed; if none given, then all accessible Data Services will be processed.')

args = parser.parse_args()

profile = args.profile[0]
host = args.host[0]
ds_list = args.ds_id

config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.ascend/credentials"))

access_id = config.get(profile, "ascend_access_key_id")
secret_key = config.get(profile, "ascend_secret_access_key")

DOCROOT="./docs/Ascend/"

A = Client(host, access_id, secret_key)

if len(ds_list) == 0:
    list_raw = A.list_data_services(raw=True)
    ds_list = list(map(lambda d: d["id"], list_raw))

pubs = A.list_data_feeds(raw=True)
pub_by_uuid = dict(map(lambda p: (p['uuid'], ".".join([p['fromOrgId'], p['id']])), pubs))
pub_name_by_id = dict(map(lambda p: (".".join([p['fromOrgId'], p['id']]), p['name']), pubs))

def gen_data_service(id):

    template="""---
id: {id}
title: {name} (Data Service)
---


# {name} <small>(Data Service)</small>

Created
: {created}

Updated
: {updated}

## Description

{description}
"""
    if not os.path.isdir(DOCROOT):
        print(DOCROOT)
        os.mkdir(DOCROOT)

    dir = DOCROOT + id
    if not os.path.isdir(dir):
        print(dir)
        os.mkdir(dir)

    ds=A.get_data_service(id)
    ds_raw=A.session.get(ds.prefix)['data']

    flow_list_raw=ds.list_dataflows(raw=True)["data"]
    feed_list_raw=A.list_data_feeds(ds_raw["id"],raw=True)

    file = dir + "/index.md"
    with open(file, "w") as f:
        print(file)
        f.write(template.format(
            created=datetime.fromisoformat(ds_raw["createdAt"].replace('Z','')),
            updated=datetime.fromisoformat(ds_raw["updatedAt"].replace('Z','')),
            **ds_raw))
        if len(feed_list_raw) > 0:
            f.write('## Data Feeds\n')
            for df_raw in feed_list_raw:
                f.write("* [{name}](./{id}) defined in [{fromProjName}](./{fromProjId})\n".format(**df_raw))
        if len(flow_list_raw) > 0:
            f.write("## Dataflows\n")
            for df_raw in flow_list_raw:
                f.write("* [{name}](./{id})\n".format(**df_raw))
        f.write("## Links\n")
        f.write("* Go to [{name}](https://{host}/ui/v2/organization/{id}) in {host}.".format(
            host=host, **ds_raw))


def gen_data_feeds(ds_id):
    template="""---
id: {ds_id}.{id}
title: {name} (Data Feed)
---


# {name} <small>(Data Feed)</small>

Defined in [{fromProjName}](./{fromProjId})


"""
    feed_list_raw=A.list_data_feeds(ds_id, raw=True)

    for df_raw in feed_list_raw:
        dir = DOCROOT + ds_id + "/"
        if not os.path.isdir(dir):
            print(dir)
            os.mkdir(dir)
        file = dir + df_raw["id"] + ".md"
        with open(file, "w") as f:
            print(file)
            f.write(template.format(
                ds_id=ds_id,
                **df_raw))
            schema = df_raw["schema"]
            if schema is not None:
                f.write("## Schema\n{schema}\n".format(
                    schema=dump_schema(schema["Details"]["Map"]["field"])))


def gen_dataflows(ds_id):
    template="""---
id: {ds_id}.{id}
title: {name} (Dataflow)
---


# {name} <small>(Dataflow)</small>

Created
: {created}

Updated
: {updated}

## Description

{description}
"""
    ds=A.get_data_service(ds_id)
    flow_list_raw=ds.list_dataflows(raw=True)["data"]
    ds_raw=A.session.get(ds.prefix)['data']

    for id in map(lambda d: d["id"], flow_list_raw):
        dir = DOCROOT + ds_raw["id"] + "/" + id + "/"
        df_raw = ds.get_dataflow(id, raw=True)["data"]
        df = ds.get_dataflow(id)
        list_raw=df.list_components(raw=True)["data"]
        if not os.path.isdir(dir):
            print(dir)
            os.mkdir(dir)
        file = dir + "index.md"
        with open(file, "w") as f:
            print(file)
            f.write(template.format(
                ds_id=ds_raw["id"],
                created=datetime.fromisoformat(ds_raw["createdAt"].replace('Z','')),
                updated=datetime.fromisoformat(ds_raw["updatedAt"].replace('Z','')),
                **df_raw))
            if list_raw is not None and len(list_raw) > 0:
                f.write("## Components\n")
                for cmp_raw in list_raw:
                    f.write("* [{name}](./{id})\n".format(**cmp_raw))
            f.write("## Links\n")
            f.write("* Go to [{name}](https://{host}/ui/v2/organization/{ds_id}/project/{id}) in {host}.".format(
                host=host, ds_id=ds_raw["id"], **df_raw))


def pick (obj, *keys):
    return {k: obj[k] for k in keys if k in obj}

def component_defn(df, id, recursive=False):
    json = flatten_values(df.get_component(id, raw=True))
    type = convert_type(json['type'])
    if type == 'connector':
        defn = connector_defn(df, json)
    elif type == 'transform':
        defn = transform_defn(df, json)
    elif type == 'data_feed':
        defn = datafeed_defn(df, json)
    else:
        defn = json

    return defn

def convert_type (t):
    mapping = {
        'source': 'connector',
        'sink': 'connector',
        'view': 'transform',
        'pub': 'data_feed',
        'sub': 'data_feed'
    }
    return mapping.get(t, t)

def flatten_singletons(d, key):
    if isinstance(d, list):
        return [flatten_singletons(v, key) for v in d]
    elif isinstance(d, dict):
        if len(d) == 1 and key in d:
            return d[key]
        else:
            return {k: flatten_singletons(v, key) for (k,v) in d.items()}
    else:
        return d

def flatten_values(d):
    return flatten_singletons(d, 'value')

def pub_id(sub):
    uuid = sub['pub']['uuid']
    return pub_by_uuid[uuid]

def list_subs_raw(df):
    return A.session.get(df.prefix + '/subs')

def get_subscriber_map(df):
    return dict(map(lambda c: (c['uuid'], pub_id(c)), list_subs_raw(df)['data']))

def get_component_map(df):
    return dict(map(lambda c: (c['uuid'], c['id']), df.list_components(raw=True)['data']))

def get_uuid_map(df):
    return dict(
        get_component_map(df),
        **get_subscriber_map(df))

def connector_defn(df, json):
    if 'source' in json:
        defn = source_defn(json['source'])
    elif 'sink' in json:
        by_uuid=get_uuid_map(df)
        defn = sink_defn(json['sink'], by_uuid[json['inputUUID']])
    return defn

def source_defn(json):
    defn = pick(json, 'container')
    if 'bytes' in json:
        ptype = list(json['bytes']['parser'].keys())[0]
        pbody = json['bytes']['parser'][ptype]
        defn['read_bytes'] = dict(
            pick(json, 'pattern'),
            parser={ ptype: pick(pbody, 'delimiter', 'columns') },
            **pick(pbody,'schema'))
    if 'records' in json:
        defn['read_records'] = dict(
            pick(json, 'pattern'),
            **pick(json['records'], 'schema'))
    defn = dict(
        defn,
        **pick(json, 'assigned_priority'))
    return defn

def transform_defn(df, json):
    by_uuid = get_uuid_map(df)
    inputs = list(map(lambda input: by_uuid[input['uuid']], json['inputs']))
    return view_defn(json['view'], inputs)


def view_defn(json, inputs):
    defn = {
        'inputs': inputs,
        'operator': json['operator']
    }
    return defn

def sink_defn(json, input):
    ctype = list(json['container'].keys())[0]
    cbody = json['container'][ctype]
    defn = {
        'input': input,
        'container': {
            ctype: pick(cbody, 'bucket', 'project', 'credential_id', 'credentials',
                        'dataset_prefix', 'prefix', 'location_template', 'staging_container')
        }
    }
    if 'bytes' in json:
        defn['write_bytes'] = dict(
            pick(json['bytes'],'formatter', 'content_encoding'),
            **pick(cbody, 'location_suffix', 'manifest', 'write_part_files'))
    if 'records' in json:
        defn['write_records'] = json['records']
    defn = dict(
        defn,
        **pick(json, 'assigned_priority'))
    return defn

def datafeed_defn(df, json):
    by_uuid = get_uuid_map(df)
    return pub_defn(json, by_uuid[json['inputUUID']])

def pub_defn(json, input):
    return {'producer': input}


def dump_field(f):
    template="| {name} | {type} |"
    return template.format(
        name=f['name'],
        type=list(f['schema']['Details'].keys())[0].replace('_',''))

def dump_schema(fields):
    header="""
| Column Name | Type |
|-------------|------|
"""
    return header + '\n'.join(map(dump_field, fields)) + '\n'

def dump_row(r, columns):
    null = {'Kind': {'Null': "`NULL`"}}
    row = r['fields']
    values = map(lambda key: list(r['fields'].get(key, null)['Kind'].values())[0], columns)
    return '|' + '|'.join(map(lambda v: " {s} ".format(s=str(v)), values)) + '|'

def dump_table(rows, fields):
    columns = list(map(lambda f: f['name'], fields))
    header = '|' + '|'.join(map(lambda s: " {s} ".format(s=s), columns)) + '|\n'
    bar = '|' + '|'.join(map(lambda s: "-------", columns)) + '|\n'
    return header + bar + '\n'.join(map(lambda row: dump_row(row, columns), rows))

def gen_components(ds_id):

    template="""---
id: {ds_id}.{df_id}.{id}
title: {name} ({ctype})
---

# {name} <small>({ctype})</small>

Created
: {created}

Updated
: {updated}

## Description

{description}

{details}
"""

    ds=A.get_data_service(ds_id)
    ds_raw=A.session.get(ds.prefix)['data']
    flow_list_raw=ds.list_dataflows(raw=True)["data"]

    map_types={
        "view": "Transform",
        "source": "ReadConnector",
        "sink": "WriteConnector",
        "pub": "Data Feed"
    }

    for df_id in map(lambda d: d["id"], flow_list_raw):
        dir = DOCROOT + ds_id + "/" + df_id + "/"
        df = ds.get_dataflow(df_id)
        list_raw=df.list_components(raw=True)["data"]
        name_by_id={}


        def linkto(id):
            if '.' in id:
                # reference to a data feed external to this dataflow
                return "[{name}]({url})".format(
                    name=name_by_id.get(id, pub_name_by_id.get(id, '(No name)')),
                    url="/Ascend/" + id.replace('.', '/'))
            else:
                # reference to component within this dataflow
                return "[{name}]({url})".format(
                    name=name_by_id.get(id, pub_name_by_id.get(id, '(No name)')),
                    url="../" + id)

        if list_raw is not None and len(list_raw) > 0:
            for c in list_raw:
                name_by_id[c['id']] = c['name']
            if not os.path.isdir(dir):
                print(dir)
                os.mkdir(dir)
            for cmp_raw in list_raw:
                id = cmp_raw['id']
                cmp = df.get_component(id)
                defn = component_defn(df, id)
                details=[]

                if cmp.component_type in ['view', 'source']:
                    try:
                        sample = A.session.get(cmp.prefix + '/records', {'limit': 10})
                        sample = sample['data']['data']

                        if 'records' in sample:
                            schema = sample['schema']['field']
                            table = sample['records']['value']
                            details.append("## Sample Data\n{records}\n\n".format(
                                records=dump_table(table, schema)))

                        if 'schema' in sample:
                            schema = sample['schema']['field']
                            details.append("## Schema\n{schema}\n".format(
                                schema=dump_schema(schema)))
                    except HTTPError:
                        pass

                if 'inputs' in defn:
                    inputs = defn['inputs']
                    details.append("## Inputs\n{ul}\n".format(
                        ul='\n'.join(map(lambda i: '* ' + linkto(i), inputs))))

                if 'operator' in defn:
                    op = defn['operator']
                    lang = "none"
                    code = "-- No code found"
                    details.append("## Code <small>({lang})</small>\n```\n{code}\n```\n".format(
                        lang = 'PySpark' if 'spark_function' in op else 'SQL',
                        code = op['spark_function']['executable']['code']['source']['inline'] if 'spark_function' in op else op['sql_query']['sql']
                    ))
                file = dir + id + ".md"
                with open(file, "w") as f:
                    print(file)
                    f.write(template.format(
                        ds_id=ds_id,
                        df_id=df_id,
                        # definition=dump_yaml(defn),
                        details='\n'.join(details),
                        ctype=map_types.get(cmp_raw['type'], 'Component'),
                        created=datetime.fromisoformat(cmp_raw["createdAt"].replace('Z','')),
                        updated=datetime.fromisoformat(cmp_raw["updatedAt"].replace('Z','')),
                        **cmp_raw))
                    f.write("## Links\n")
                    f.write("* Go to [{name}](https://{host}/ui/v2/organization/{ds_id}/project/{df_id}/{type}/{id}) in {host}.".format(
                        host=host, ds_id=ds_id, df_id=df_id, **cmp_raw))

# main loop

for id in ds_list:
    gen_data_service(id)
    gen_data_feeds(id)
    gen_dataflows(id)
    gen_components(id)

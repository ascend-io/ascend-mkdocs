# Ascend MkDocs

A script to generate [MkDocs](https://www.mkdocs.org) documentation for Data Services
deployed in Ascend.

## Installation

The script is a Python 3 script, and the Ascend SDK for Python
only supports Python 3, so verify you are running Python 3.

```
==> python --version
Python 3.7.3
```


You will need to install the
[Ascend SDK for Python](https://github.com/ascend-io/sdk-python):

```
==> pip install git+https://github.com/ascend-io/sdk-python.git

```

You will also need [MkDocs](https://www.mkdocs.org):

```
==> pip install mkdocs
```

## Credentials

The script connects to Ascend using the SDL, and authenticates using Access Keys,
which you can generate once you are logged into your account.
If you do not already have an account on Ascend, you can
[request a free trial](https://www.ascend.io/get-started/).

You can run this script using either a
[Service Account Key](https://developer.ascend.io/docs/service-accounts) or a
[Developer Key](https://developer.ascend.io/docs/developer-keys).
If you use a Service Account Key, you will be limited to accessing only
the Data Service under which the Service Account is defined.
Using a Developer Key gives you all the same access permissions as your user account.

Once you've generated your key (see the documentation links above),
you will store it in the file `~/.ascend/credentials` on the system
where you will be running the script.
This file is in config file format (same as Amazon's credentials file), which
allows you to store many key (id, secret) pairs, identifying them with profiles.
By default, we will name the profile `ascend`.

After inserting your Access Keys,
your `~/.ascend/credentials` file should look something like this:

```
[ascend]
ascend_access_key_id=IAM0AN1ACCESS2KEY3ID
ascend_secret_access_key=thI5iSThe53Cret0Acc35SKEySoDo1ntGive1tUp
```

## Running `md_dump.py`

The `md_dump.py` script walks Data Services in Ascend and generates Markdown
files for each Data Service, Dataflow, Data Feed, and Component.

```
==> python ./md_dump.py --help
usage: md_dump.py [-h] [--host HOST] [--profile PROFILE] [ds_id [ds_id ...]]


positional arguments:
  ds_id              IDs of Data Services to be processed; if none given, then
                     all accessible Data Services will be processed.

optional arguments:
  -h, --help         show this help message and exit
  --host HOST        The hostname of the Ascend environment to connect to
  --profile PROFILE  The profile used to locate Ascend credentials in
                     ~/.ascend/credentials
```

If your account is on `trial.ascend.io` and you save your Access Key
under the `ascend` profile, you will not need to specify `--host`
and `--profiles` as those are the defaults.

```
==> python ./md_dump.py My_Data_Service
./docs/Ascend/My_Data_Service/index.md
⋮
```

> You can find the ID of your Data Service on the Integrations Tab for
> the Data Service.

The `md_dump.py` script prints out the names of the Markdown files it generates.
The files will be created in the directory `./docs/Ascend` which is where
`mkdocs` expects them.


## Running `mkdocs`

Once the tree of Markdown files has been generated, you can start the
`mkdocs` server.

```
==> mkdocs serve
INFO    -  Building documentation...
INFO    -  Cleaning site directory
[I 191015 19:19:51 server:296] Serving on http://127.0.0.1:8000
[I 191015 19:19:51 handlers:62] Start watching changes
[I 191015 19:19:51 handlers:64] Start detecting changes
```

Visit your new `mkdocs` site at `http://localhost:8000`.

## Next Steps

* You can run `md_dump.py` again, and the `mkdocs` server will detect
  new files and update the served pages.

* `mkdocs` can also be used to generate a static web site which
  can be served using a standard HTTP server.

* Fork this repo and make enhancements. We encourage PRs from
  the Ascend community.

* We are actively developing the
  [Ascend SDK](https://github.com/ascend-io/sdk-python)
  in ways that will simplfy creation of workflows and utilities such as this.  Check back soon for updates.

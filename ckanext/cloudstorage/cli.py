#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import os.path
import cgi
import tempfile
import click

from ckanapi import LocalCKAN
from ckanext.cloudstorage.storage import (
    CloudStorage,
    ResourceCloudStorage
)
from ckanext.cloudstorage.model import (
    create_tables,
    drop_tables
)
from ckan.logic import NotFound
 
@click.group(short_help=u"Perform commands in the cloudstorage.")
def cloudstorage():
    """Perform commands in the cloudstorage.
    """
    pass

class FakeFileStorage(cgi.FieldStorage):
    def __init__(self, fp, filename):
        self.file = fp
        self.filename = filename

@cloudstorage.command(name='migrate', short_help=u'Upload local storage to the remote.')
@click.argument(u'path_to_storage', required=True, type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.argument(u'resource_id', required=False)
def migrate(path_to_storage, resource_id):
    lc = LocalCKAN()
    resources = {}
    failed = []

    # The resource folder is stuctured like so on disk:
    # - storage/
    #   - ...
    # - resources/
    #   - <3 letter prefix>
    #     - <3 letter prefix>
    #       - <remaining resource_id as filename>
    #       ...
    #     ...
    #   ...
    for root, dirs, files in os.walk(path_to_storage):
        # Only the bottom level of the tree actually contains any files. We
        # don't care at all about the overall structure.
        if not files:
            continue

        split_root = root.split('/')
        resource_id = split_root[-2] + split_root[-1]

        for file_ in files:
            ckan_res_id = resource_id + file_
            if resource_id and ckan_res_id != resource_id:
                continue

            resources[ckan_res_id] = os.path.join(
                root,
                file_
            )

    for i, resource in enumerate(resources.items(), 1):
        resource_id, file_path = resource
        click.echo('[{i}/{count}] Working on {id}'.format(
            i=i,
            count=len(resources),
            id=resource_id
        ))

        try:
            resource = lc.action.resource_show(id=resource_id)
        except NotFound:
            click.echo(u'\tResource not found')
            continue
        if resource['url_type'] != 'upload':
            click.echo(u'\t`url_type` is not `upload`. Skip')
            continue

        with open(file_path, 'rb') as fin:
            resource['upload'] = FakeFileStorage(
                fin,
                resource['url'].split('/')[-1]
            )
            try:
                uploader = ResourceCloudStorage(resource)
                uploader.upload(resource['id'])
            except Exception as e:
                failed.append(resource_id)
                click.echo(u'\tError of type {0} during upload: {1}'.format(type(e), e))

    if failed:
        log_file = tempfile.NamedTemporaryFile(delete=False)
        log_file.file.writelines(failed)
        click.echo(u'ID of all failed uploads are saved to `{0}`'.format(log_file.name))


@cloudstorage.command(name='fix-cors', short_help=u'Update CORS rules where possible.')
@click.argument(u'allowed_origins', nargs=-1)
def fix_cors(allowed_origins):
    cs = CloudStorage()

    if cs.can_use_advanced_azure:
        from azure.storage import blob as azure_blob
        from azure.storage import CorsRule

        blob_service = azure_blob.BlockBlobService(
            cs.driver_options['key'],
            cs.driver_options['secret']
        )

        blob_service.set_blob_service_properties(
            cors=[
                CorsRule(
                    allowed_origins=allowed_origins,
                    allowed_methods=['GET']
                )
            ]
        )
        click.echo('Done!')
    else:
        click.echo(
            'The driver {driver_name} being used does not currently'
            ' support updating CORS rules through'
            ' cloudstorage.'.format(
                driver_name=cs.driver_name
            )
        )

@cloudstorage.command(name='initdb', short_help=u'Reinitalize database tables.')
def initdb():
    drop_tables()
    create_tables()
    click.echo("DB tables are reinitialized")

def get_commands():
    return [cloudstorage]
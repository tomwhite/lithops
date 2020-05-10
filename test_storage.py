def test_localhost_storage():
    from pywren_ibm_cloud.storage.backends.localhost import StorageBackend
    config = {}
    storage = StorageBackend(config)
    bucket = "bucket-not-used"
    
    storage.put_object(bucket, "dir1/key1", "v1")
    storage.put_object(bucket, "dir2/key1", "v1")
    storage.put_object(bucket, "dir2/key2", "v2")

    assert storage.list_keys(bucket, "dir1") == ["dir1/key1"]

    assert storage.get_object(bucket, "dir1/key1") == b"v1"

    assert len(storage.list_keys(bucket, "dir1")) == 1
    storage.delete_object(bucket, "dir1/key1")
    assert len(storage.list_keys(bucket, "dir1")) == 0

    assert len(storage.list_keys(bucket, "dir2")) == 2
    storage.delete_objects(bucket, ["dir2/key1", "dir2/key2"])
    assert len(storage.list_keys(bucket, "dir2")) == 0

def test_gcsfs_storage():
    from pywren_ibm_cloud.storage.backends.gcsfs import StorageBackend
    config = {
        "project_id": "tom-white"
    }
    storage = StorageBackend(config)
    bucket = "pywren-tw"

    storage.put_object(bucket, "dir1/key1", "v1")
    storage.put_object(bucket, "dir2/key1", "v1")
    storage.put_object(bucket, "dir2/key2", "v2")

    assert storage.list_keys(bucket, "dir1") == ["dir1/key1"]

    assert storage.get_object(bucket, "dir1/key1") == b"v1"

    assert len(storage.list_keys(bucket, "dir1")) == 1
    storage.delete_object(bucket, "dir1/key1")
    assert len(storage.list_keys(bucket, "dir1")) == 0

    assert len(storage.list_keys(bucket, "dir2")) == 2
    storage.delete_objects(bucket, ["dir2/key1", "dir2/key2"])
    assert len(storage.list_keys(bucket, "dir2")) == 0

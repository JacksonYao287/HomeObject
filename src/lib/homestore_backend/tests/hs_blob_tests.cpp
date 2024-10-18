#include "homeobj_fixture.hpp"

#include "lib/homestore_backend/index_kv.hpp"
#include "generated/resync_pg_shard_generated.h"
#include "generated/resync_blob_data_generated.h"

TEST_F(HomeObjectFixture, BasicEquivalence) {
    auto shard_mgr = _obj_inst->shard_manager();
    auto pg_mgr = _obj_inst->pg_manager();
    auto blob_mgr = _obj_inst->blob_manager();
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(shard_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(pg_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(blob_mgr.get()));
}

TEST_F(HomeObjectFixture, BasicPutGetDelBlobWRestart) {
    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;
    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::vector< std::pair< pg_id_t, shard_id_t > > pg_shard_id_vec;
    blob_map_t blob_map;

    // Create blob size in range (1, 16kb) and user key in range (1, 1kb)
    const uint32_t max_blob_size = 16 * 1024;

    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = create_shard(i, 64 * Mi);
            pg_shard_id_vec.emplace_back(i, shard.id);
            LOGINFO("pg {} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blob(blob_map, pg_shard_id_vec, num_blobs_per_shard, max_blob_size);

    // Verify all get blobs
    verify_get_blob(blob_map);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // for (uint64_t i = 1; i <= num_pgs; i++) {
    //     r_cast< HSHomeObject* >(_obj_inst.get())->print_btree_index(i);
    // }

    LOGINFO("Flushing CP.");
    trigger_cp(true /* wait */);

    // Restart homeobject
    g_helper->restart();
    g_helper->sync_for_test_start();

    // Verify all get blobs after restart
    verify_get_blob(blob_map);

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // Put blob after restart to test the persistance of blob sequence number
    put_blob(blob_map, pg_shard_id_vec, num_blobs_per_shard, max_blob_size);

    // Verify all get blobs with random offset and length.
    verify_get_blob(blob_map, true /* use_random_offset */);

    // Verify the stats after put blobs after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, false /* deleted */);

    // Delete all blobs
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->del(shard_id, blob_id).get();
        ASSERT_TRUE(g);
        LOGINFO("delete blob shard {} blob {}", shard_id, blob_id);
    }

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);

    // Delete again should have no errors.
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->del(shard_id, blob_id).get();
        ASSERT_TRUE(g);
        LOGINFO("delete blob shard {} blob {}", shard_id, blob_id);
    }

    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);

    // After delete all blobs, get should fail
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
        ASSERT_TRUE(!g);
    }

    // all the deleted blobs should be tombstone in index table
    auto hs_homeobject = dynamic_cast< HSHomeObject* >(_obj_inst.get());
    for (const auto& [id, blob] : blob_map) {
        int64_t pg_id = std::get< 0 >(id), shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        shared< BlobIndexTable > index_table;
        {
            std::shared_lock lock_guard(hs_homeobject->_pg_lock);
            auto iter = hs_homeobject->_pg_map.find(pg_id);
            ASSERT_TRUE(iter != hs_homeobject->_pg_map.end());
            index_table = static_cast< HSHomeObject::HS_PG* >(iter->second.get())->index_table_;
        }

        auto g = hs_homeobject->get_blob_from_index_table(index_table, shard_id, blob_id);
        ASSERT_FALSE(!!g);
        EXPECT_EQ(BlobErrorCode::UNKNOWN_BLOB, g.error().getCode());
    }

    LOGINFO("Flushing CP.");
    trigger_cp(true /* wait */);

    // Restart homeobject
    restart();

    // After restart, for all deleted blobs, get should fail
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
        ASSERT_TRUE(!g);
    }

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);
}

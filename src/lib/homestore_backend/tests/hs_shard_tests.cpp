#include "homeobj_fixture.hpp"

TEST_F(HomeObjectFixture, CreateMultiShards) {
    pg_id_t pg_id{1};
    create_pg(pg_id);
    auto _shard_1 = create_shard(pg_id, 64 * Mi);
    auto _shard_2 = create_shard(pg_id, 64 * Mi);

    auto chunk_num_1 = _obj_inst->get_shard_chunk(_shard_1.id);
    ASSERT_TRUE(chunk_num_1.has_value());

    auto chunk_num_2 = _obj_inst->get_shard_chunk(_shard_2.id);
    ASSERT_TRUE(chunk_num_2.has_value());

    // check if both chunk is on the same pdev;
    auto alloc_hint1 = _obj_inst->chunk_selector()->chunk_to_hints(chunk_num_1.value());
    auto alloc_hint2 = _obj_inst->chunk_selector()->chunk_to_hints(chunk_num_2.value());
    ASSERT_TRUE(alloc_hint1.pdev_id_hint.has_value());
    ASSERT_TRUE(alloc_hint2.pdev_id_hint.has_value());
    ASSERT_TRUE(alloc_hint1.pdev_id_hint.value() == alloc_hint2.pdev_id_hint.value());
}

TEST_F(HomeObjectFixture, CreateMultiShardsOnMultiPG) {
    std::vector< homeobject::pg_id_t > pgs;

    for (pg_id_t pg{1}; pg < 4; pg++) {
        create_pg(pg);
        pgs.push_back(pg);
    }

    for (const auto pg : pgs) {
        auto shard_info = create_shard(pg, Mi);
        auto chunk_num_1 = _obj_inst->get_shard_chunk(shard_info.id);
        ASSERT_TRUE(chunk_num_1.has_value());

        // create another shard again.
        shard_info = create_shard(pg, Mi);
        auto chunk_num_2 = _obj_inst->get_shard_chunk(shard_info.id);
        ASSERT_TRUE(chunk_num_2.has_value());

        // check if both chunk is on the same pdev;
        auto alloc_hint1 = _obj_inst->chunk_selector()->chunk_to_hints(chunk_num_1.value());
        auto alloc_hint2 = _obj_inst->chunk_selector()->chunk_to_hints(chunk_num_2.value());
        ASSERT_TRUE(alloc_hint1.pdev_id_hint.has_value());
        ASSERT_TRUE(alloc_hint2.pdev_id_hint.has_value());
        ASSERT_TRUE(alloc_hint1.pdev_id_hint.value() == alloc_hint2.pdev_id_hint.value());
    }
}

TEST_F(HomeObjectFixture, SealShard) {
    pg_id_t pg_id{1};
    create_pg(pg_id);
    auto shard_info = create_shard(pg_id, 64 * Mi);
    ASSERT_EQ(ShardInfo::State::OPEN, shard_info.state);

    // seal the shard
    shard_info = seal_shard(shard_info.id);
    ASSERT_EQ(ShardInfo::State::SEALED, shard_info.state);
}

TEST_F(HomeObjectFixture, ShardManagerRecovery) {
    pg_id_t pg_id{1};
    create_pg(pg_id);

    // create one shard;
    auto shard_info = create_shard(pg_id, Mi);
    auto shard_id = shard_info.id;
    EXPECT_EQ(ShardInfo::State::OPEN, shard_info.state);
    EXPECT_EQ(Mi, shard_info.total_capacity_bytes);
    EXPECT_EQ(Mi, shard_info.available_capacity_bytes);
    EXPECT_EQ(0ul, shard_info.deleted_capacity_bytes);
    EXPECT_EQ(pg_id, shard_info.placement_group);

    // restart homeobject and check if pg/shard info will be recovered.
    restart();

    // check PG after recovery.
    EXPECT_TRUE(_obj_inst->_pg_map.size() == 1);
    auto pg_iter = _obj_inst->_pg_map.find(pg_id);
    EXPECT_TRUE(pg_iter != _obj_inst->_pg_map.end());
    auto& pg_result = pg_iter->second;
    EXPECT_EQ(1, pg_result->shards_.size());
    // verify the sequence number is correct after recovery.
    EXPECT_EQ(1, pg_result->shard_sequence_num_);
    // check recovered shard state.
    auto hs_shard = d_cast< homeobject::HSHomeObject::HS_Shard* >(pg_result->shards_.front().get());
    auto& recovered_shard_info = hs_shard->info;
    verify_hs_shard(recovered_shard_info, shard_info);

    // seal the shard when shard is recovery
    shard_info = seal_shard(shard_id);

    LOGINFO("gggg 2222");
    EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);

    // restart again to verify the shards has expected states.
    restart();

    auto s = _obj_inst->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);
    LOGINFO("gggg 3333");
    EXPECT_EQ(ShardInfo::State::SEALED, s.value().state);
    pg_iter = _obj_inst->_pg_map.find(pg_id);
    // verify the sequence number is correct after recovery.
    LOGINFO("gggg 4444");
    EXPECT_EQ(1, pg_iter->second->shard_sequence_num_);

    // re-create new shards on this pg works too even homeobject is restarted twice.
    auto new_shard_info = create_shard(pg_id, Mi);
    EXPECT_NE(shard_id, new_shard_info.id);
    LOGINFO("gggg 5555");
    EXPECT_EQ(ShardInfo::State::OPEN, new_shard_info.state);
    EXPECT_EQ(2, pg_iter->second->shard_sequence_num_);
}

TEST_F(HomeObjectFixture, SealedShardRecovery) {
    pg_id_t pg_id{1};
    create_pg(pg_id);

    // create one shard and seal it.
    auto shard_info = create_shard(pg_id, Mi);
    auto shard_id = shard_info.id;
    shard_info = seal_shard(shard_id);
    EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);

    // check the shard info from ShardManager to make sure on_commit() is successfully.
    auto pg_iter = _obj_inst->_pg_map.find(pg_id);
    EXPECT_TRUE(pg_iter != _obj_inst->_pg_map.end());
    auto& pg_result = pg_iter->second;
    EXPECT_EQ(1, pg_result->shards_.size());
    auto hs_shard = d_cast< homeobject::HSHomeObject::HS_Shard* >(pg_result->shards_.front().get());
    EXPECT_EQ(ShardInfo::State::SEALED, hs_shard->info.state);

    // release the homeobject and homestore will be shutdown automatically.
    LOGI("restart home_object");
    restart();

    // re-create the homeobject and pg infos and shard infos will be recover automatically.
    EXPECT_TRUE(_obj_inst->_pg_map.size() == 1);
    // check shard internal state;
    pg_iter = _obj_inst->_pg_map.find(pg_id);
    EXPECT_TRUE(pg_iter != _obj_inst->_pg_map.end());
    EXPECT_EQ(1, pg_iter->second->shards_.size());
    hs_shard = d_cast< homeobject::HSHomeObject::HS_Shard* >(pg_iter->second->shards_.front().get());
    auto& recovered_shard_info = hs_shard->info;
    verify_hs_shard(recovered_shard_info, shard_info);
}

TEST_F(HomeObjectFixture, SealShardWithRestart) {
    // Create a pg, shard, put blob should succeed, seal and put blob again should fail.
    // Recover and put blob again should fail.
    pg_id_t pg_id{1};
    create_pg(pg_id);

    auto s = _obj_inst->shard_manager()->create_shard(pg_id, 64 * Mi).get();
    ASSERT_TRUE(!!s);
    auto shard_info = s.value();
    auto shard_id = shard_info.id;
    s = _obj_inst->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);

    LOGINFO("Got shard {}", shard_id);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::OPEN);
    auto b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
    ASSERT_TRUE(!!b);
    LOGINFO("Put blob {}", b.value());

    s = _obj_inst->shard_manager()->seal_shard(shard_id).get();
    ASSERT_TRUE(!!s);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::SEALED);
    LOGINFO("Sealed shard {}", shard_id);

    b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
    ASSERT_TRUE(!b);
    ASSERT_EQ(b.error().getCode(), BlobErrorCode::SEALED_SHARD);
    LOGINFO("Put blob {}", b.error());

    // Restart homeobject
    restart();

    // Verify shard is sealed.
    s = _obj_inst->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);

    LOGINFO("After restart shard {}", shard_id);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::SEALED);

    b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
    ASSERT_TRUE(!b);
    ASSERT_EQ(b.error().getCode(), BlobErrorCode::SEALED_SHARD);
    LOGINFO("Put blob {}", b.error());
}

TEST_F(HomeObjectFixture, PGBlobIterator) {
    uint64_t num_shards_per_pg = 3;
    uint64_t num_blobs_per_shard = 5;
    std::vector< std::pair< pg_id_t, shard_id_t > > pg_shard_id_vec;
    blob_map_t blob_map;

    // Create blob size in range (1, 16kb) and user key in range (1, 1kb)
    const uint32_t max_blob_size = 16 * 1024;

    create_pg(1 /* pg_id */);
    for (uint64_t j = 0; j < num_shards_per_pg; j++) {
        auto shard = _obj_inst->shard_manager()->create_shard(1 /* pg_id */, 64 * Mi).get();
        ASSERT_TRUE(!!shard);
        pg_shard_id_vec.emplace_back(1, shard->id);
        LOGINFO("pg {} shard {}", 1, shard->id);
    }

    // Put blob for all shards in all pg's.
    put_blob(blob_map, pg_shard_id_vec, num_blobs_per_shard, max_blob_size);

    auto ho = dynamic_cast< homeobject::HSHomeObject* >(_obj_inst.get());
    PG* pg1;
    {
        auto lg = std::shared_lock(ho->_pg_lock);
        auto iter = ho->_pg_map.find(1);
        ASSERT_TRUE(iter != ho->_pg_map.end());
        pg1 = iter->second.get();
    }

    auto pg1_iter = std::make_shared< homeobject::HSHomeObject::PGBlobIterator >(*ho, pg1->pg_info_.replica_set_uuid);
    ASSERT_EQ(pg1_iter->end_of_scan(), false);

    // Verify PG shard meta data.
    sisl::io_blob_safe meta_blob;
    pg1_iter->create_pg_shard_snapshot_data(meta_blob);
    ASSERT_TRUE(meta_blob.size() > 0);

    auto pg_req = GetSizePrefixedResyncPGShardInfo(meta_blob.bytes());
    ASSERT_EQ(pg_req->pg()->pg_id(), pg1->pg_info_.id);
    auto u1 = pg_req->pg()->replica_set_uuid();
    auto u2 = pg1->pg_info_.replica_set_uuid;
    ASSERT_EQ(std::string(u1->begin(), u1->end()), std::string(u2.begin(), u2.end()));

    // Verify get blobs for pg.
    uint64_t max_num_blobs_in_batch = 3, max_batch_size_bytes = 128 * Mi;
    std::vector< HSHomeObject::BlobInfoData > blob_data_vec;
    while (!pg1_iter->end_of_scan()) {
        std::vector< HSHomeObject::BlobInfoData > vec;
        bool end_of_shard;
        auto result = pg1_iter->get_next_blobs(max_num_blobs_in_batch, max_batch_size_bytes, vec, end_of_shard);
        ASSERT_EQ(result, 0);
        for (auto& v : vec) {
            blob_data_vec.push_back(std::move(v));
        }
    }

    ASSERT_EQ(blob_data_vec.size(), num_shards_per_pg * num_blobs_per_shard);
    for (auto& b : blob_data_vec) {
        auto g = _obj_inst->blob_manager()->get(b.shard_id, b.blob_id, 0, 0).get();
        ASSERT_TRUE(!!g);
        auto result = std::move(g.value());
        LOGINFO("Get blob pg {} shard {} blob {} len {} data {}", 1, b.shard_id, b.blob_id, b.blob.body.size(),
                hex_bytes(result.body.cbytes(), 5));
        EXPECT_EQ(result.body.size(), b.blob.body.size());
        EXPECT_EQ(std::memcmp(result.body.bytes(), b.blob.body.cbytes(), result.body.size()), 0);
        EXPECT_EQ(result.user_key.size(), b.blob.user_key.size());
        EXPECT_EQ(result.user_key, b.blob.user_key);
        EXPECT_EQ(result.object_off, b.blob.object_off);
    }
}

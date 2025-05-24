#include "homeobj_fixture.hpp"

TEST_F(HomeObjectFixture, BasicGC) {
    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;
    auto num_blobs_per_shard = 10 * SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;

    for (uint16_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        pg_blob_id[i] = 0;
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = create_shard(i, 64 * Mi);
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Delete all blobs
    del_all_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, true /* deleted */);

    // seal all shards
    for (const auto& [_, shard_vec] : pg_shard_id_vec) {
        for (const auto& shard_id : shard_vec) {
            auto shard_info = seal_shard(shard_id);
            EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);
        }
    }

    bool all_deleted_blobs_has_been_gc{true};
    auto chunk_selector = _obj_inst->chunk_selector();

    while (true) {
        for (uint16_t i = 1; i <= num_pgs; i++) {
            auto pg_chunks = chunk_selector->get_pg_chunks(i);
            for (uint64_t j = 0; j < pg_chunks->size(); j++) {
                auto EXVchunk = chunk_selector->get_extend_vchunk(pg_chunks->at(j));
                const auto available_blk = EXVchunk->available_blks();
                const auto total_blks = EXVchunk->get_total_blks();
                if (available_blk != total_blks) {
                    LOGERROR("pg={},  chunk={} is not empty, available_blk={}, total_blk={}", i,
                             EXVchunk->get_chunk_id(), available_blk, total_blks);
                    all_deleted_blobs_has_been_gc = false;
                    break;
                }
            }
            if (!all_deleted_blobs_has_been_gc) break;
        }
        if (all_deleted_blobs_has_been_gc) break;
        all_deleted_blobs_has_been_gc = true;
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

#if 0
TEST_F(HomeObjectFixture, BasicEGC) {
    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;

    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;

    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        pg_blob_id[i] = 0;
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = create_shard(i, 64 * Mi);
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Delete all blobs
    del_all_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, true /* deleted */);

    // do not seal all shards and sumbit gc task directly, so the shard is still open and egc will be triggered
}
#endif
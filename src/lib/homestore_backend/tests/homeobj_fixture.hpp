#pragma once
#include <chrono>
#include <cmath>
#include <mutex>

#include <boost/uuid/random_generator.hpp>
#include <gtest/gtest.h>

#include <homestore/homestore.hpp>
// will allow unit tests to access object private/protected for validation;
#define protected public
#define private public

#include "lib/homestore_backend/hs_homeobject.hpp"
#include "bits_generator.hpp"
#include "hs_repl_test_helper.hpp"

using namespace std::chrono_literals;

using homeobject::BlobError;
using homeobject::PGError;
using homeobject::PGInfo;
using homeobject::PGMember;
using homeobject::ShardError;
using namespace homeobject;

#define hex_bytes(buffer, len) fmt::format("{}", spdlog::to_hex((buffer), (buffer) + (len)))

extern std::unique_ptr< test_common::HSReplTestHelper > g_helper;

class HomeObjectFixture : public ::testing::Test {
public:
    std::shared_ptr< homeobject::HSHomeObject > _obj_inst;
    std::random_device rnd{};
    std::default_random_engine rnd_engine{rnd()};

    void SetUp() override {
        g_helper->sync_for_test_start();
        _obj_inst = std::dynamic_pointer_cast< HSHomeObject >(g_helper->build_new_homeobject());
    }

    void TearDown() override {
        g_helper->sync_for_cleanup_start();
        _obj_inst.reset();
        g_helper->delete_homeobject();
    }

    void restart() {
        _obj_inst.reset();
        _obj_inst = std::dynamic_pointer_cast< HSHomeObject >(g_helper->restart());
        g_helper->sync_for_test_start();
    }

    // schedule create_pg to replica_num
    void create_pg(pg_id_t pg_id, uint32_t replica_num = 0) {
        // g_helper->sync_for_create_pg();
        if (pg_exist(pg_id)) {
            LOGINFO("PG {} already exists", pg_id);
            return;
        }

        if (replica_num == g_helper->replica_num()) {
            auto memebers = g_helper->members();
            auto name = g_helper->name();
            auto info = homeobject::PGInfo(pg_id);
            for (const auto& member : memebers) {
                if (replica_num == member.second) {
                    info.members.insert(homeobject::PGMember{member.first, name + std::to_string(member.second), 1});
                } else {
                    info.members.insert(homeobject::PGMember{member.first, name + std::to_string(member.second), 0});
                }
            }
            auto p = _obj_inst->pg_manager()->create_pg(std::move(info)).get();
            ASSERT_TRUE(p);
        } else {
            // follower need to wait for pg creation to complete
            while (!pg_exist(pg_id)) {
                LOGINFO("follower is waiting for pg {} created", pg_id);
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            LOGINFO("pg {} is created at follower", pg_id);
        }
    }

    ShardInfo create_shard(pg_id_t pg_id, uint64_t size_bytes) {
        g_helper->sync_for_create_shard();
        if (!pg_exist(pg_id)) {
            LOGERROR("PG {} does not exist", pg_id);
            return {};
        }
        PGStats pg_stats;
        auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
        RELEASE_ASSERT(res, "can not get pg {} stats", pg_id);
        LOGINFO("start creating shard at pg {} leader {}", pg_id, pg_stats.leader_id);
        if (g_helper->my_replica_id() == pg_stats.leader_id) {
            auto s = _obj_inst->shard_manager()->create_shard(pg_id, size_bytes).get();
            RELEASE_ASSERT(!!s, "failed to create shard");
            auto ret = s.value();
            g_helper->set_shard_id(ret.id);
            LOGINFO("shard {} is created at leader, gloabl shard id {}", ret.id, g_helper->get_shard_id());
            return ret;
        } else {
            while (g_helper->get_shard_id() == INVALID_SHARD_ID) {
                LOGINFO("follower is waiting for new shard created at leader, global shard id {}",
                        g_helper->get_shard_id());
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            auto shard_id = g_helper->get_shard_id();
            LOGINFO("follower find new shard created at leader, global shard id {}", g_helper->get_shard_id());
            while (!shard_exist(shard_id)) {
                LOGINFO("follower is waiting for shard {} created locally", shard_id);
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }

            auto r = _obj_inst->shard_manager()->get_shard(shard_id).get();
            RELEASE_ASSERT(!!r, "failed to get shard {}", shard_id);
            return r.value();
        }
    }

    ShardInfo seal_shard(shard_id_t shard_id) {
        g_helper->sync_for_create_shard();
        if (!shard_exist(shard_id)) {
            LOGERROR("shard {} does not exist", shard_id);
            return {};
        }
        auto r = _obj_inst->shard_manager()->get_shard(shard_id).get();
        RELEASE_ASSERT(!!r, "failed to get shard {}", shard_id);
        auto pg_id = r.value().placement_group;
        PGStats pg_stats;
        auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
        RELEASE_ASSERT(res, "can not get pg {} stats", pg_id);
        LOGINFO("start sealing shard at pg {} leader {}", pg_id, pg_stats.leader_id);

        if (g_helper->my_replica_id() == pg_stats.leader_id) {
            auto s = _obj_inst->shard_manager()->seal_shard(shard_id).get();
            RELEASE_ASSERT(!!s, "failed to seal shard");
            auto ret = s.value();
            g_helper->set_shard_id(ret.id);
            LOGINFO("shard {} is sealed at leader, gloabl shard id {}", ret.id, g_helper->get_shard_id());
            return ret;
        } else {
            while (g_helper->get_shard_id() == INVALID_SHARD_ID) {
                LOGINFO("follower is waiting for seal shard scheduled at leader, global shard id {}",
                        g_helper->get_shard_id());
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            auto shard_id = g_helper->get_shard_id();
            LOGINFO("follower find new shard when sealing shard at leader, global shard id {}",
                    g_helper->get_shard_id());
            while (true) {
                auto r = _obj_inst->shard_manager()->get_shard(shard_id).get();
                RELEASE_ASSERT(!!r, "failed to get shard {}", shard_id);
                auto shard_info = r.value();
                if (shard_info.state == ShardInfo::State::SEALED) { return shard_info; }
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
        }
    }

    void put_blob(shard_id_t shard_id, Blob&& blob) {
        g_helper->sync_for_put_blob();
        if (!shard_exist(shard_id)) {
            LOGERROR("shard {} does not exist", shard_id);
            return;
        }
        auto r = _obj_inst->shard_manager()->get_shard(shard_id).get();
        RELEASE_ASSERT(!!r, "failed to get shard {}", shard_id);
        auto pg_id = r.value().placement_group;
        PGStats pg_stats;
        auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
        RELEASE_ASSERT(res, "can not get pg {} stats", pg_id);

        if (g_helper->my_replica_id() == pg_stats.leader_id) {
            auto b =
                _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
            RELEASE_ASSERT(!!b, "failed to pub blob");
            LOGINFO("leader put new blob {}", b.value());
            g_helper->set_blob_id(b.value());
            LOGINFO("leader put new blob, global blob id {}", g_helper->get_blob_id());
        } else {
            while (g_helper->get_blob_id() == INVALID_BLOB_ID) {
                LOGINFO("follower waiting for blob id {}", g_helper->get_blob_id());
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            auto blob_id = g_helper->get_blob_id();
            LOGINFO("follower succeed waiting for blob id {}", g_helper->get_blob_id());
            while (!blob_exist(shard_id, blob_id)) {
                LOGINFO("follower is waiting for shard {} blob {} created locally", shard_id, blob_id);
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
        }
    }

    static void trigger_cp(bool wait) {
        auto fut = homestore::hs()->cp_mgr().trigger_cp_flush(true /* force */);
        auto on_complete = [&](auto success) {
            EXPECT_EQ(success, true);
            LOGINFO("CP Flush completed");
        };

        if (wait) {
            on_complete(std::move(fut).get());
        } else {
            std::move(fut).thenValue(on_complete);
        }
    }

    using blob_map_t = std::map< std::tuple< pg_id_t, shard_id_t, blob_id_t >, homeobject::Blob >;

    void put_blob(blob_map_t& blob_map, std::vector< std::pair< pg_id_t, shard_id_t > > const& pg_shard_id_vec,
                  uint64_t const num_blobs_per_shard, uint32_t const max_blob_size) {
        std::uniform_int_distribution< uint32_t > rand_blob_size{1u, max_blob_size};
        std::uniform_int_distribution< uint32_t > rand_user_key_size{1u, 1 * 1024};

        for (const auto& id : pg_shard_id_vec) {
            int64_t pg_id = id.first, shard_id = id.second;
            for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                uint32_t alignment = 512;
                // Create non 512 byte aligned address to create copy.
                if (k % 2 == 0) alignment = 256;

                std::string user_key;
                user_key.resize(rand_user_key_size(rnd_engine));
                BitsGenerator::gen_random_bits(user_key.size(), (uint8_t*)user_key.data());
                auto blob_size = rand_blob_size(rnd_engine);
                homeobject::Blob put_blob{sisl::io_blob_safe(blob_size, alignment), user_key, 42ul};
                BitsGenerator::gen_random_bits(put_blob.body);
                // Keep a copy of random payload to verify later.
                homeobject::Blob clone{sisl::io_blob_safe(blob_size, alignment), user_key, 42ul};
                std::memcpy(clone.body.bytes(), put_blob.body.bytes(), put_blob.body.size());

            retry:
                auto b = _obj_inst->blob_manager()->put(shard_id, std::move(put_blob)).get();
                if (!b && b.error().getCode() == BlobErrorCode::NOT_LEADER) {
                    LOGINFO("Failed to put blob due to not leader, sleep 1s and retry put", pg_id, shard_id);
                    put_blob = homeobject::Blob{sisl::io_blob_safe(blob_size, alignment), user_key, 42ul};
                    std::memcpy(put_blob.body.bytes(), clone.body.bytes(), clone.body.size());
                    std::this_thread::sleep_for(1s);
                    goto retry;
                }

                if (!b) {
                    LOGERROR("Failed to put blob pg {} shard {} error={}", pg_id, shard_id, b.error());
                    ASSERT_TRUE(false);
                    continue;
                }
                auto blob_id = b.value();

                LOGINFO("Put blob pg {} shard {} blob {} data {}", pg_id, shard_id, blob_id,
                        hex_bytes(clone.body.cbytes(), std::min(10u, clone.body.size())));
                blob_map.insert({{pg_id, shard_id, blob_id}, std::move(clone)});
            }
        }
    }

    void verify_get_blob(blob_map_t const& blob_map, bool const use_random_offset = false) {
        uint32_t off = 0, len = 0;
        for (const auto& [id, blob] : blob_map) {
            int64_t pg_id = std::get< 0 >(id), shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
            len = blob.body.size();
            if (use_random_offset) {
                std::uniform_int_distribution< uint32_t > rand_off_gen{0u, blob.body.size() - 1u};
                std::uniform_int_distribution< uint32_t > rand_len_gen{1u, blob.body.size()};

                off = rand_off_gen(rnd_engine);
                len = rand_len_gen(rnd_engine);
                if ((off + len) >= blob.body.size()) { len = blob.body.size() - off; }
            }

            auto g = _obj_inst->blob_manager()->get(shard_id, blob_id, off, len).get();
            ASSERT_TRUE(!!g);
            auto result = std::move(g.value());
            LOGINFO("After restart get blob pg {} shard {} blob {} off {} len {} data {}", pg_id, shard_id, blob_id,
                    off, len, hex_bytes(result.body.cbytes(), std::min(len, 10u)));
            EXPECT_EQ(result.body.size(), len);
            EXPECT_EQ(std::memcmp(result.body.bytes(), blob.body.cbytes() + off, result.body.size()), 0);
            EXPECT_EQ(result.user_key.size(), blob.user_key.size());
            EXPECT_EQ(blob.user_key, result.user_key);
            EXPECT_EQ(blob.object_off, result.object_off);
        }
    }

    void verify_obj_count(uint32_t num_pgs, uint32_t shards_per_pg, uint32_t blobs_per_shard,
                          bool deleted_all = false) {
        uint32_t exp_active_blobs = deleted_all ? 0 : shards_per_pg * blobs_per_shard;
        uint32_t exp_tombstone_blobs = deleted_all ? shards_per_pg * blobs_per_shard : 0;

        for (uint32_t i = 1; i <= num_pgs; ++i) {
            PGStats stats;
            _obj_inst->pg_manager()->get_stats(i, stats);
            ASSERT_EQ(stats.num_active_objects, exp_active_blobs) << "Active objs stats not correct";
            ASSERT_EQ(stats.num_tombstone_objects, exp_tombstone_blobs) << "Deleted objs stats not correct";
        }
    }

    void verify_hs_pg(HSHomeObject::HS_PG* lhs_pg, HSHomeObject::HS_PG* rhs_pg) {
        // verify index table
        EXPECT_EQ(lhs_pg->index_table_->uuid(), rhs_pg->index_table_->uuid());
        EXPECT_EQ(lhs_pg->index_table_->used_size(), rhs_pg->index_table_->used_size());

        // verify repl_dev
        EXPECT_EQ(lhs_pg->repl_dev_->group_id(), rhs_pg->repl_dev_->group_id());

        // verify pg_info_superblk
        auto lhs = lhs_pg->pg_sb_.get();
        auto rhs = rhs_pg->pg_sb_.get();

        EXPECT_EQ(lhs->id, rhs->id);
        EXPECT_EQ(lhs->num_members, rhs->num_members);
        EXPECT_EQ(lhs->replica_set_uuid, rhs->replica_set_uuid);
        EXPECT_EQ(lhs->index_table_uuid, rhs->index_table_uuid);
        EXPECT_EQ(lhs->blob_sequence_num, rhs->blob_sequence_num);
        EXPECT_EQ(lhs->active_blob_count, rhs->active_blob_count);
        EXPECT_EQ(lhs->tombstone_blob_count, rhs->tombstone_blob_count);
        EXPECT_EQ(lhs->total_occupied_blk_count, rhs->total_occupied_blk_count);
        EXPECT_EQ(lhs->tombstone_blob_count, rhs->tombstone_blob_count);
        for (uint32_t i = 0; i < lhs->num_members; i++) {
            EXPECT_EQ(lhs->members[i].id, rhs->members[i].id);
            EXPECT_EQ(lhs->members[i].priority, rhs->members[i].priority);
            EXPECT_EQ(0, std::strcmp(lhs->members[i].name, rhs->members[i].name));
        }
    }

private:
    bool pg_exist(pg_id_t pg_id) {
        std::vector< pg_id_t > pg_ids;
        _obj_inst->pg_manager()->get_pg_ids(pg_ids);
        return std::find(pg_ids.begin(), pg_ids.end(), pg_id) != pg_ids.end();
    }

    bool shard_exist(shard_id_t id) {
        auto r = _obj_inst->shard_manager()->get_shard(id).get();
        return !!r;
    }

    bool blob_exist(shard_id_t shard_id, blob_id_t blob_id) {
        auto r = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
        return !!r;
    }
};

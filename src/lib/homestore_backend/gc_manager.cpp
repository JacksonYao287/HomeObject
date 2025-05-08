#include <homestore/btree/btree_req.hpp>
#include <homestore/btree/btree_kv.hpp>

#include "hs_homeobject.hpp"
namespace homeobject {

/* GCManager */

GCManager::GCManager(std::shared_ptr< HeapChunkSelector > chunk_selector, HSHomeObject* homeobject) :
        m_chunk_selector{chunk_selector}, m_hs_home_object{homeobject} {}

GCManager::~GCManager() { stop(); }

void GCManager::start() {
    for (const auto& [pdev_id, gc_actor] : m_pdev_gc_actors) {
        gc_actor->start();
        LOGERROR("start gc actor for pdev={}", pdev_id);
    }
    m_gc_timer_hdl = iomanager.schedule_global_timer(
        GC_SCAN_INTERVAL_SEC * 1000 * 1000 * 1000, true, nullptr /*cookie*/, iomgr::reactor_regex::random_worker,
        [this](void*) { scan_chunks_for_gc(); }, false /* wait_to_schedule */);
    LOGINFO("gc scheduler timer has started, interval is set to {} seconds", GC_SCAN_INTERVAL_SEC);
}

void GCManager::stop() {
    if (m_gc_timer_hdl == iomgr::null_timer_handle) {
        LOGINFO("gc schduler timer is not running, no need to stop it");
        return;
    }

    LOGINFO("stop gc schduler timer");
    iomanager.cancel_timer(m_gc_timer_hdl, true);
    m_gc_timer_hdl = iomgr::null_timer_handle;

    for (const auto& [pdev_id, gc_actor] : m_pdev_gc_actors) {
        gc_actor->stop();
        LOGERROR("stop gc actor for pdev={}", pdev_id);
    }
}

folly::SemiFuture< bool > GCManager::submit_gc_task(task_priority priority, chunk_id_t chunk_id) {
    auto pdev_id = m_chunk_selector->get_extend_vchunk(chunk_id)->get_pdev_id();
    auto it = m_pdev_gc_actors.find(pdev_id);
    if (it == m_pdev_gc_actors.end()) {
        LOGINFO("pdev gc actor not found for pdev_id: {}", pdev_id);
        return folly::makeFuture< bool >(false);
    }
    auto& actor = it->second;
    auto [promise, future] = folly::makePromiseContract< bool >();
    actor->add_gc_task(priority, {chunk_id, static_cast< uint8_t >(priority), std ::move(promise)});
    LOGINFO("submit gc task for chunk_id: {}, pdev_id: {}", chunk_id, pdev_id);
    return std::move(future);
}

std::shared_ptr< GCManager::pdev_gc_actor >
GCManager::try_create_pdev_gc_actor(uint32_t pdev_id, std::shared_ptr< GCBlobIndexTable > index_table) {
    auto const [it, happened] = m_pdev_gc_actors.try_emplace(
        pdev_id, std::make_shared< pdev_gc_actor >(pdev_id, m_chunk_selector, index_table, m_hs_home_object));
    RELEASE_ASSERT((it != m_pdev_gc_actors.end()), "Unexpected error in m_pdev_gc_actors!!!");
    if (happened) {
        LOGINFO("create new gc actor for pdev_id: {}", pdev_id);
    } else {
        LOGINFO("pdev gc actor already exists for pdev_id: {}", pdev_id);
    }
    return it->second;
}

std::shared_ptr< GCManager::pdev_gc_actor > GCManager::get_pdev_gc_actor(uint32_t pdev_id) {
    auto it = m_pdev_gc_actors.find(pdev_id);
    if (it == m_pdev_gc_actors.end()) {
        LOGERROR("pdev gc actor not found for pdev_id: {}", pdev_id);
        return nullptr;
    }
    return it->second;
}

bool GCManager::is_eligible_for_gc(std::shared_ptr< HeapChunkSelector::ExtendedVChunk > chunk) {
    if (!chunk->m_pg_id.has_value()) return false;
    auto defrag_blk_num = chunk->get_defrag_nblks();
    auto total_blk_num = chunk->get_total_blks();

    // defrag_blk_num > (GC_THRESHOLD_PERCENT/100) * total_blk_num, to avoid floating point number calculation
    // TODO: avoid overflow here.
    return 100 * defrag_blk_num > total_blk_num * GC_GARBAGE_RATE_THRESHOLD;
}

void GCManager::scan_chunks_for_gc() {
    for (const auto& [_, chunk] : m_chunk_selector->get_all_chunks()) {
        if (is_eligible_for_gc(chunk)) {
            auto pdev_id = chunk->get_pdev_id();
            auto it = m_pdev_gc_actors.find(pdev_id);
            if (it != m_pdev_gc_actors.end()) {
                auto& actor = it->second;
                auto [promise, _ /* future */] = folly::makePromiseContract< bool >();
                // TODO:: use the future to check if the normal task is completed when necessary
                actor->add_gc_task(
                    task_priority::normal,
                    {chunk->get_chunk_id(), static_cast< uint8_t >(task_priority::normal), std ::move(promise)});
            }
        }
    }
}

/* pdev_gc_actor */

GCManager::pdev_gc_actor::pdev_gc_actor(uint32_t pdev_id, std::shared_ptr< HeapChunkSelector > chunk_selector,
                                        std::shared_ptr< GCBlobIndexTable > index_table, HSHomeObject* homeobject) :
        m_pdev_id{pdev_id},
        m_chunk_selector{chunk_selector},
        // we have a queue for each priority, so the size is the same as the number of priorities
        m_gc_task_queue{static_cast< size_t >(task_priority::priority_count)},
        m_reserved_chunk_queue{RESERVED_CHUNK_NUM_PER_PDEV},
        m_index_table{index_table},
        m_hs_home_object{homeobject} {

    RELEASE_ASSERT(index_table, "index_table for a gc_actor should not be nullptr!!!");
}

void GCManager::pdev_gc_actor::start() {
    bool stopped = true;
    if (!m_is_stopped.compare_exchange_strong(stopped, false, std::memory_order_release, std::memory_order_relaxed)) {
        LOGERROR("pdev gc actor for pdev_id={} is already started, no need to start again!", m_pdev_id);
        return;
    }
    // thread number is the same as reserved chunk, which can make sure every gc thread can take a reserved chunk
    // for gc
    m_gc_executor = std::make_shared< folly::IOThreadPoolExecutor >(RESERVED_CHUNK_NUM_PER_PDEV);
    for (uint32_t i = 0; i < RESERVED_CHUNK_NUM_PER_PDEV; i++) {
        m_gc_executor->add([this] {
            while (!m_is_stopped.load(std::memory_order_relaxed)) {
                GCManager::gc_task task;
                if (m_gc_task_queue.try_dequeue(task)) {
                    process_gc_task(task);
                } else {
                    // if we use a condition variable(cv), when stop, the cv might lost the notification. so, it`s
                    // better to do sleep and check
                    LOGINFO("no gc task found, sleep 1s!");
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            }
        });
    }

    LOGINFO("pdev gc actor for pdev_id={} has started", m_pdev_id);
}

void GCManager::pdev_gc_actor::stop() {
    bool stopped = false;
    if (!m_is_stopped.compare_exchange_strong(stopped, true, std::memory_order_release, std::memory_order_relaxed)) {
        LOGERROR("pdev gc actor for pdev_id={} is already stopped, no need to stop again!", m_pdev_id);
        return;
    }
    m_gc_executor->stop();
    m_gc_executor->join();
    m_gc_executor.reset();
    LOGINFO("pdev gc actor for pdev_id={} has stopped", m_pdev_id);
}

void GCManager::pdev_gc_actor::add_reserved_chunk(chunk_id_t chunk_id) {
    m_reserved_chunk_queue.blockingWrite(chunk_id);
}

void GCManager::pdev_gc_actor::add_gc_task(task_priority priority, GCManager::gc_task gc_task) {
    m_gc_task_queue.at_priority(static_cast< size_t >(priority)).enqueue(std::move(gc_task));
}

// this method is expected to be called sequentially when replaying metablk, so we don't need to worry about the
// concurrency issue.
void GCManager::pdev_gc_actor::handle_recovered_gc_task(const GCManager::gc_task_superblk* gc_task) {
    chunk_id_t move_from_chunk = gc_task->move_from_chunk;
    chunk_id_t move_to_chunk = gc_task->move_to_chunk;
    uint32_t pdev_id = gc_task->pdev_id;
    uint8_t priority = gc_task->priority;

    // 1 we need to move the move_to_chunk out of the reserved chunk queue
    std::list< chunk_id_t > reserved_chunks;
    chunk_id_t chunk_id;
    for (; m_reserved_chunk_queue.read(chunk_id);) {
        if (chunk_id == move_to_chunk) {
            // we found the chunk to be moved, so we can stop reading
            break;
        }
        reserved_chunks.emplace_back(chunk_id);
    }
    // now we need to put the reserved chunks back to the reserved chunk queue
    for (const auto& reserved_chunk : reserved_chunks) {
        m_reserved_chunk_queue.blockingWrite(reserved_chunk);
    }

    // 2 we need to select the move_from_chunk out of per pg chunk heap in chunk selector if it is a gc task with normal
    // priority. for the task with emergent priority, it is already selected since it is now used for an open shard.
    if (priority == static_cast< uint8_t >(task_priority::normal)) {
        auto vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);
        RELEASE_ASSERT(pdev_id == vchunk->get_pdev_id(), "pdev_id={} in superblk is expected to be equal to pdev_id={}",
                       pdev_id, vchunk->get_pdev_id());
        RELEASE_ASSERT(vchunk, "can not find vchunk for pchunk {} in chunk selector!", move_from_chunk);
        RELEASE_ASSERT(vchunk->m_pg_id.has_value(), "chunk_id={} is expected to belong to a pg, but not !",
                       move_from_chunk);
        RELEASE_ASSERT(vchunk->available(), "pg_id={}, chunk_id={} is expected to be available, but not!",
                       vchunk->m_pg_id.value(), move_from_chunk);
        // we can safely remove the chunk from the chunk selector, since it is not used by any open shard now.
        m_chunk_selector->select_specific_chunk(vchunk->m_pg_id.value(), vchunk->m_v_chunk_id.value());
    }

    // 3 now we can switch the two chunks.
    replace_blob_index(move_from_chunk, move_to_chunk, priority);
}

void GCManager::pdev_gc_actor::replace_blob_index(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk,
                                                  uint8_t priority) {
    // 1 get all blob index from gc index table
    std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > > out_vector;
    auto start_key = BlobRouteByChunkKey{BlobRouteByChunk(move_to_chunk, 0, 0)};
    auto end_key = BlobRouteByChunkKey{BlobRouteByChunk{move_to_chunk, std::numeric_limits< uint64_t >::max(),
                                                        std::numeric_limits< uint64_t >::max()}};
    homestore::BtreeQueryRequest< BlobRouteByChunkKey > query_req{homestore::BtreeKeyRange< BlobRouteByChunkKey >{
        std::move(start_key), true /* inclusive */, std::move(end_key), true /* inclusive */
    }};

    auto const ret = m_index_table->query(query_req, out_vector);
    if (ret != homestore::btree_status_t::success) {
        // "ret != homestore::btree_status_t::has_more" is not expetced here, since we are querying all the pbas in one
        // time.
        // TODO:: handle the error case here.
        RELEASE_ASSERT(false, "Failed to query blobs in index table for ret={} move_to_chunk={}", ret, move_to_chunk);
    }

    // 2 get pg index table
    auto move_from_vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);
    RELEASE_ASSERT(move_from_vchunk->m_pg_id.has_value(), "chunk_id={} is expected to belong to a pg, but not!",
                   move_from_chunk);
    auto pg_id = move_from_vchunk->m_pg_id.value();
    auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    RELEASE_ASSERT(hs_pg, "Unknown PG for pg_id={}", pg_id);
    auto pg_index_table = hs_pg->index_table_;
    RELEASE_ASSERT(pg_index_table, "Index table not found for PG pg_id={}", pg_id);

    // 3 update pg index table according to the query result of gc index table.
    // BtreeRangePutRequest only support update a range of keys to the same value, so we need to update the pg
    // indextable here one by one. since the update of index table is very fast , and gc is not time sensitive, so
    // for now we do this sequentially.
    // TODO:: concurrently update pg index table if necessary

    for (const auto& [k, v] : out_vector) {
        BlobRouteKey index_key{BlobRoute{k.key().shard, k.key().blob}};

        homestore::BtreeSinglePutRequest update_req{
            &index_key, &v, homestore::btree_put_type::UPDATE, nullptr,
            [](homestore::BtreeKey const& key, homestore::BtreeValue const& value_in_btree,
               homestore::BtreeValue const& new_value) -> homestore::put_filter_decision {
                BlobRouteValue existing_value{value_in_btree};
                if (existing_value.pbas() == HSHomeObject::tombstone_pbas) {
                    //  if the blob has been deleted and the value is tombstone,
                    //  we should not use a valid pba to update a tombstone.
                    return homestore::put_filter_decision::keep;
                }
                return homestore::put_filter_decision::replace;
            }};

        auto status = pg_index_table->put(update_req);
        if (status != homestore::btree_status_t::success && status != homestore::btree_status_t::filtered_out) {
            // TODO:: handle the error case here.
            RELEASE_ASSERT(false, "can not update pg index table, pg_id={}, move_from_chunk_id={}", pg_id,
                           move_from_chunk);
        }
    }

    //  4 change the pg_id and vchunk_id of the move_to_chunk according to move_from_chunk
    auto move_to_vchunk = m_chunk_selector->get_extend_vchunk(move_to_chunk);
    move_to_vchunk->m_pg_id = move_from_vchunk->m_pg_id;
    move_to_vchunk->m_v_chunk_id = move_from_vchunk->m_v_chunk_id;
    move_to_vchunk->m_state =
        priority == static_cast< uint8_t >(task_priority::normal) ? ChunkState::AVAILABLE : ChunkState::INUSE;

    // TODO:: need to make sure the pg_id of the exvchunk of move_from_chunk is empty, so that it will not be selected
    // as a gc candidate chunk.

    // TODO:
    //  5 push the move_from_chunk to the reserved chunk queue and the move_to_chunk to the pg heap.
    //  6 update the reserved chunk of this gc actor, metablk
    //  7 update the chunk list of pg, metablk
    //  8 change chunk->shard list map
}

sisl::sg_list GCManager::pdev_gc_actor::generate_shard_super_blk_sg_list(shard_id_t shard_id) {
    // TODO: do the buffer check before using it.
    auto raw_shard_sb = m_hs_home_object->_get_hs_shard(shard_id);
    RELEASE_ASSERT(raw_shard_sb, "can not find shard super blk for shard_id={} !!!", shard_id);

    const auto shard_sb =
        const_cast< HSHomeObject::HS_Shard* >(d_cast< const HSHomeObject::HS_Shard* >(raw_shard_sb))->sb_.get();

    auto blk_size = homestore::data_service().get_blk_size();
    auto shard_sb_size = sizeof(HSHomeObject::shard_info_superblk);
    auto total_size = sisl::round_up(shard_sb_size, blk_size);
    auto shard_sb_buf = iomanager.iobuf_alloc(blk_size, total_size);

    std::memcpy(shard_sb_buf, shard_sb, shard_sb_size);

    sisl::sg_list shard_sb_sgs;
    shard_sb_sgs.size = total_size;
    shard_sb_sgs.iovs.emplace_back(iovec{.iov_base = shard_sb_buf, .iov_len = total_size});
    return shard_sb_sgs;
}

bool GCManager::pdev_gc_actor::copy_valid_data(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk, bool is_emergent) {
    // TODO:: add an is_empty fuction for chunk
    auto move_to_chunk_total_blks = m_chunk_selector->get_extend_vchunk(move_to_chunk)->get_total_blks();
    auto move_to_chunk_available_blks = m_chunk_selector->get_extend_vchunk(move_to_chunk)->available_blks();
    RELEASE_ASSERT(move_to_chunk_total_blks == move_to_chunk_available_blks,
                   "move_to_chunk should be empty, total_blks={}, available_blks={}, move_to_chunk_id={}",
                   move_to_chunk_total_blks, move_to_chunk_available_blks, move_to_chunk);

    auto shards = m_hs_home_object->get_shards_in_chunk(move_from_chunk);
    if (shards.empty()) {
        LOGWARN("no shard found in move_from_chunk, chunk_id={}, ", move_from_chunk);
        return true;
    }

    auto& data_service = homestore::data_service();

    homestore::blk_alloc_hints hints;
    hints.chunk_id_hint = move_to_chunk;
    homestore::MultiBlkId out_blkids;

    const auto last_shard_id = *(shards.rbegin());
    const auto& shard_info = m_hs_home_object->_get_hs_shard(last_shard_id)->info;
    const auto& shard_state = shard_info.state;

    // the last shard that triggers emergent gc should be in open state
    if (shard_state != ShardInfo::State::OPEN && is_emergent) {
        LOGERROR("shard state is not open for emergent gc, shard_id={} !!!", last_shard_id);
        return false;
    }

    auto pg_index_table = m_hs_home_object->get_hs_pg(shard_info.placement_group)->index_table_;
    auto blk_size = data_service.get_blk_size();

    for (const auto& shard_id : shards) {
        bool is_last_shard = (shard_id == last_shard_id);
        std::vector< std::pair< BlobRouteKey, BlobRouteValue > > out_vector;
        auto start_key = BlobRouteKey{BlobRoute{shard_id, std::numeric_limits< uint64_t >::min()}};
        auto end_key = BlobRouteKey{BlobRoute{shard_id, std::numeric_limits< uint64_t >::max()}};

        homestore::BtreeQueryRequest< BlobRouteKey > query_req{
            homestore::BtreeKeyRange< BlobRouteKey >{std::move(start_key), true /* inclusive */, std::move(end_key),
                                                     true /* inclusive */},
            // blob count in a shard will not exceed uint32_t_max
            homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY, std::numeric_limits< uint32_t >::max()};

        auto const status = pg_index_table->query(query_req, out_vector);
        if (status != homestore::btree_status_t::success && status != homestore::btree_status_t::has_more) {
            LOGERROR("Failed to query blobs in index table for status={} shard={}", status, shard_id);
            return false;
        }

        if (out_vector.empty()) {
            LOGINFO("empty shard found in move_from_chunk, chunk_id={}, shard_id={}", move_from_chunk, shard_id);
            // TODO::send a delete shard request to raft channel. there is a case that when we are doing gc, the
            // shard becomes empty, need to handle this case
            continue;
        }

        // prepare a shard header for this shard in move_to_chunk
        sisl::sg_list header_sgs = generate_shard_super_blk_sg_list(shard_id);
        // now, the written header is the same as footer, so we lost the original header.
        // TODO: update the following to the correct value, especially lsn. this probably impact the accuracy of metrics
        // report.
        /*
        header->info.state = ShardInfo::State::OPEN;
            uint64_t lsn; // created_lsn
            uint64_t created_time;
            uint64_t last_modified_time;
            uint64_t available_capacity_bytes;
            uint64_t total_capacity_bytes;
            uint64_t deleted_capacity_bytes;
        */

        // TODO::involve ratelimiter in the folloing code, where read/write are scheduled
        auto succeed_copying_shard =
            // 1 write the shard header to move_to_chunk
            data_service.async_alloc_write(header_sgs, hints, out_blkids)
                .thenValue([this, &hints, &move_to_chunk, &move_from_chunk, &is_emergent, &is_last_shard, &shard_id,
                            &blk_size, &out_vector, &data_service, header_sgs = std::move(header_sgs)](auto&& err) {
                    RELEASE_ASSERT(header_sgs.iovs.size() == 1, "header_sgs.iovs.size() should be 1, but not!");
                    iomanager.iobuf_free(reinterpret_cast< uint8_t* >(header_sgs.iovs[0].iov_base));
                    if (err) {
                        LOGERROR("Failed to write shard header for move_to_chunk={} shard_id={}, err={}", move_to_chunk,
                                 shard_id, err.value());
                        return folly::makeFuture< bool >(false);
                    }

                    std::vector< folly::Future< bool > > futs;

                    // 2 copy all the valid blob in the shard from move_from_chunk to move_to_chunk
                    for (const auto& [k, v] : out_vector) {
                        // k is shard_id + blob_id, v is multiblk id
                        auto pba = v.pbas();
                        auto total_size = pba.blk_count() * blk_size;

                        // buffer for read and write
                        sisl::sg_list data_sgs;
                        data_sgs.size = total_size;
                        data_sgs.iovs.emplace_back(
                            iovec{.iov_base = iomanager.iobuf_alloc(blk_size, total_size), .iov_len = total_size});

                        futs.emplace_back(std::move(
                            // read blob from move_from_chunk
                            data_service.async_read(pba, data_sgs, total_size)
                                .thenValue([this, &k, &hints, &move_from_chunk, &move_to_chunk, &data_service,
                                            data_sgs = std::move(data_sgs)](auto&& err) {
                                    RELEASE_ASSERT(data_sgs.iovs.size() == 1,
                                                   "data_sgs.iovs.size() should be 1, but not!");
                                    if (err) {
                                        LOGERROR("Failed to read blob from move_from_chunk={}, shard_id={}, "
                                                 "blob_id={}: err={}",
                                                 move_from_chunk, k.key().shard, k.key().blob, err.value());
                                        iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                                        return folly::makeFuture< bool >(false);
                                    }

                                    // write the blob to the move_to_chunk. we do not care about the blob order in a
                                    // shard since we can not guarantee a certain order
                                    homestore::MultiBlkId new_pba;
                                    return data_service.async_alloc_write(data_sgs, hints, new_pba)
                                        .thenValue([this, &k, &new_pba, &move_to_chunk,
                                                    data_sgs = std::move(data_sgs)](auto&& err) {
                                            RELEASE_ASSERT(data_sgs.iovs.size() == 1,
                                                           "data_sgs.iovs.size() should be 1, but not!");
                                            iomanager.iobuf_free(
                                                reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                                            if (err) {
                                                LOGERROR("Failed to write blob to move_to_chunk={}, shard_id={}, "
                                                         "blob_id={}, err={}",
                                                         move_to_chunk, k.key().shard, k.key().blob, err.value());
                                                return false;
                                            }

                                            // insert a new entry to gc index table for this blob. [move_to_chunk_id,
                                            // shard_id, blob_id] -> [new pba]
                                            BlobRouteByChunkKey key{
                                                BlobRouteByChunk{move_to_chunk, k.key().shard, k.key().blob}};
                                            BlobRouteValue value{new_pba}, existing_value;

                                            homestore::BtreeSinglePutRequest put_req{
                                                &key, &value, homestore::btree_put_type::INSERT, &existing_value};
                                            auto status = m_index_table->put(put_req);
                                            if (status != homestore::btree_status_t::success) {
                                                LOGERROR("Failed to insert new key to gc index table for "
                                                         "move_to_chunk={}, shard_id={}, blob_id={}, err={}",
                                                         move_to_chunk, k.key().shard, k.key().blob, status);
                                                return false;
                                            }
                                            LOGDEBUG("successfully insert new key to gc index table for "
                                                     "move_to_chunk={}, shard_id={}, blob_id={}",
                                                     move_to_chunk, k.key().shard, k.key().blob);
                                            return true;
                                        });
                                })));
                    }

                    // 3 write a shard footer for this shard
                    sisl::sg_list footer_sgs = generate_shard_super_blk_sg_list(shard_id);
                    homestore::MultiBlkId out_blkids;

                    return folly::collectAllUnsafe(futs)
                        .thenValue([this, &is_emergent, &is_last_shard, &shard_id, &blk_size, &hints, &move_to_chunk,
                                    &data_service, footer_sgs](auto&& results) {
                            // throw exception in this part so that we can skip async_alloc_write in some cases.
                            // the errod in the throw exception does not exactly stand for the real cause.
                            for (auto const& ok : results) {
                                RELEASE_ASSERT(ok.hasValue(), "we never throw any exception when copying data");
                                if (!ok.value()) {
                                    // if any op fails, we drop this gc task.
                                    return folly::makeFuture< std::error_code >(
                                        std::make_error_code(std::errc::bad_address));
                                }
                            }

                            // the chunk that triggers emergent gc should be the last shard in the chunk, and the last
                            // shard should be open, so skip writing the footer for this case.
                            if (is_emergent && is_last_shard) {
                                LOGINFO("skip writing the footer for move_to_chunk={} shard_id={} for emergent gc task",
                                        move_to_chunk, shard_id);
                                return folly::makeFuture< std::error_code >(std::error_code{});
                            }
                            homestore::MultiBlkId out_blkids;
                            return data_service.async_alloc_write(footer_sgs, hints, out_blkids);
                        })
                        .thenValue([this, &move_to_chunk, &shard_id, footer_sgs](auto&& err) {
                            RELEASE_ASSERT(footer_sgs.iovs.size() == 1, "footer_sgs.iovs.size() should be 1, but not!");
                            iomanager.iobuf_free(reinterpret_cast< uint8_t* >(footer_sgs.iovs[0].iov_base));

                            if (err) {
                                LOGERROR("Failed to write shard footer for move_to_chunk={} shard_id={}, "
                                         "err={}",
                                         move_to_chunk, shard_id, err.value());
                                return false;
                            }
                            return true;
                        });
                })
                .get();

        if (!succeed_copying_shard) {
            LOGERROR("Failed to copy all blobs from move_from_chunk={} to move_to_chunk={} for shard_id={}",
                     move_from_chunk, move_to_chunk, shard_id);
            return false;
        }

        LOGINFO("successfully copy blobs from move_from_chunk={} to move_to_chunk={} for shard_id={}", move_from_chunk,
                move_to_chunk, shard_id);
    }

    LOGINFO("all valid blobs are copied from move_from_chunk={} to move_to_chunk={}", move_from_chunk, move_to_chunk);
    return true;
}

void GCManager::pdev_gc_actor::purge_reserved_chunk(chunk_id_t chunk) {
    auto vchunk = m_chunk_selector->get_extend_vchunk(chunk);
    RELEASE_ASSERT(!vchunk->m_pg_id.has_value(), "chunk_id={} is expected to a reserved chunk, and not belong to a pg",
                   chunk);
    vchunk->reset(); // reset the chunk to make sure it is empty

    // clear all the entries of this chunk in the gc index table
    auto start_key = BlobRouteByChunkKey{BlobRouteByChunk(chunk, 0, 0)};
    auto end_key = BlobRouteByChunkKey{
        BlobRouteByChunk{chunk, std::numeric_limits< uint64_t >::max(), std::numeric_limits< uint64_t >::max()}};
    homestore::BtreeRangeRemoveRequest< BlobRouteByChunkKey > range_remove_req{
        homestore::BtreeKeyRange< BlobRouteByChunkKey >{
            std::move(start_key), true /* inclusive */, std::move(end_key), true /* inclusive */
        }};
    auto status = m_index_table->remove(range_remove_req);
    if (status != homestore::btree_status_t::success) {
        // TODO:: handle the error case here!
        RELEASE_ASSERT(false, "can not clear gc index table, chunk_id={}", chunk);
    }
}

void GCManager::pdev_gc_actor::process_gc_task(gc_task& task) {
    auto priority = task.get_priority();
    auto move_from_chunk = task.get_move_from_chunk_id();
    auto move_from_vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);

    // we need to select the move_from_chunk out of the per pg chunk heap in chunk selector if it is a gc task with
    // normal priority. for the task with emergent priority, it is already selected since it is now used for an open
    // shard.
    if (priority == static_cast< uint8_t >(task_priority::normal)) {
        if (!move_from_vchunk->m_pg_id.has_value()) {
            LOGWARN("move_from_chunk={} is expected to belong to a pg, but not!", move_from_chunk);
            task.complete(false);
            return;
        }
        auto selected_chunk = m_chunk_selector->select_specific_chunk(move_from_vchunk->m_pg_id.value(),
                                                                      move_from_vchunk->m_v_chunk_id.value());
        if (!selected_chunk) {
            LOGWARN("move_from_chunk={} is expected to be selected, but not!", move_from_chunk);
            task.complete(false);
            return;
        }
    }

    chunk_id_t move_to_chunk;
    // wait for a reserved chunk to be available
    m_reserved_chunk_queue.blockingRead(move_to_chunk);
    LOGINFO("gc task for move_from_chunk={} to move_to_chunk={} with priority={} start copying data", move_from_chunk,
            move_to_chunk, priority);

    purge_reserved_chunk(move_to_chunk);

    if (!copy_valid_data(move_from_chunk, move_to_chunk)) {
        LOGWARN("failed to copy data from move_from_chunk={} to move_to_chunk={} with priority={}", move_from_chunk,
                move_to_chunk, priority);
        task.complete(false);
        return;
    }

    // trigger cp to make sure the offset the the append blk allocator and the wbcache of gc index table are both
    // flushed.
    auto fut = homestore::hs()->cp_mgr().trigger_cp_flush(true /* force */);
    LOGINFO("CP Flush {} after copy data from move_from_chunk {} to move_to_chunk {} with priority={}",
            std::move(fut).get() ? "success" : "failed", move_from_chunk, move_to_chunk, priority);

    // after data copy, we need persist the gc task meta blk, so that if crash happens, we can recover it after
    // restart.
    homestore::superblk< GCManager::gc_task_superblk > gc_task_sb{GCManager::_gc_task_meta_name};
    gc_task_sb.create(sizeof(GCManager::gc_task_superblk));
    gc_task_sb->pdev_id = m_pdev_id;
    gc_task_sb->move_from_chunk = move_from_chunk;
    gc_task_sb->move_to_chunk = move_to_chunk;
    gc_task_sb->priority = priority;
    // write the gc task meta blk to the meta service, so that it can be recovered after restart
    gc_task_sb.write();

    // after the data copy is done, we can switch the two chunks.
    LOGINFO("gc task for move_from_chunk={} to move_to_chunk={} with priority={} start switching chunk",
            move_from_chunk, move_to_chunk, priority);
    replace_blob_index(move_from_chunk, move_to_chunk, priority);

    // now we can complete the task. for emergent gc, we need wait for the gc task to be completed
    gc_task_sb.destroy();
    task.complete(true);
    LOGINFO("gc task for move_from_chunk={} to move_to_chunk={} with priority={} is completed", move_from_chunk,
            move_to_chunk, priority);
}

GCManager::pdev_gc_actor::~pdev_gc_actor() {
    stop();
    LOGINFO("pdev gc actor for pdev_id={} is destroyed", m_pdev_id);
}

/* RateLimiter */
GCManager::RateLimiter::RateLimiter(uint64_t refill_count_per_second) :
        tokens_(refill_count_per_second), refillRate_(refill_count_per_second) {
    lastRefillTime_ = std::chrono::steady_clock::now();
}

bool GCManager::RateLimiter::allowRequest(uint64_t count) {
    std::lock_guard lock(mutex_);
    refillTokens();
    if (tokens_ >= count) {
        tokens_ -= count;
        return true;
    }
    return false;
}

void GCManager::RateLimiter::refillTokens() {
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast< std::chrono::seconds >(now - lastRefillTime_).count();
    if (duration) {
        tokens_ = refillRate_;
        lastRefillTime_ = now;
    }
}

} // namespace homeobject

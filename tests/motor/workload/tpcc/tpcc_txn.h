// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <memory>

#include "process/txn.h"
#include "tpcc/tpcc_db.h"

/******************** The business logic (Transaction) start ********************/

// The following transaction business logics are referred to the standard TPCC specification.

/* TPC BENCHMARK™ C
** Standard Specification
** Revision 5.11
** February 2010
** url: http://tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf
*/

// Note: Remote hash slot limits the insertion number. For a 20-slot bucket, the uppper bound is 44744 new order.
bool TxNewOrder(TPCC* tpcc_client,
                FastRandom* random_generator,
                coro_yield_t& yield,
                tx_id_t tx_id,
                TXN* txn);

bool TxPayment(TPCC* tpcc_client,
               FastRandom* random_generator,
               coro_yield_t& yield,
               tx_id_t tx_id,
               TXN* txn);

bool TxDelivery(TPCC* tpcc_client,
                FastRandom* random_generator,
                coro_yield_t& yield,
                tx_id_t tx_id,
                TXN* txn);

bool TxOrderStatus(TPCC* tpcc_client,
                   FastRandom* random_generator,
                   coro_yield_t& yield,
                   tx_id_t tx_id,
                   TXN* txn);

bool TxStockLevel(TPCC* tpcc_client,
                 FastRandom* random_generator,
                 coro_yield_t& yield,
                 tx_id_t tx_id,
                 TXN* txn);

// After every worker has stopped committing. Reads through the live QP and
// prints TPCC_CHECK / TPCC_CHECK_FAIL. Does not abort the process.
void CheckTpccConsistency(TPCC* tpcc_client, coro_yield_t& yield, TXN* txn);

/******************** The business logic (Transaction) end ********************/
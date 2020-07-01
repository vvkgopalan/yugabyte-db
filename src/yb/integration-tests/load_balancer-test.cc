// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include <gtest/gtest.h>

#include "yb/integration-tests/yb_table_test_base.h"

#include "yb/consensus/consensus.pb.h"
#include "yb/consensus/consensus.proxy.h"
#include "yb/gutil/strings/join.h"
#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/external_mini_cluster.h"
#include "yb/integration-tests/cluster_verifier.h"
#include "yb/master/master.h"
#include "yb/master/master-test-util.h"
#include "yb/master/sys_catalog.h"
#include "yb/master/master.proxy.h"
#include "yb/rpc/messenger.h"
#include "yb/rpc/rpc_controller.h"
#include "yb/tools/yb-admin_client.h"

namespace yb {
namespace integration_tests {

constexpr uint32_t kDefaultTimeoutMillis = 30000;

class LoadBalancerTest : public YBTableTestBase {
 protected:
  void SetUp() override {
    YBTableTestBase::SetUp();

    yb_admin_client_ = std::make_unique<tools::enterprise::ClusterAdminClient>(
        external_mini_cluster()->GetMasterAddresses(), kDefaultTimeoutMillis);

    ASSERT_OK(yb_admin_client_->Init());
  }

  bool use_external_mini_cluster() override { return true; }

  int num_tablets() override {
    return 4;
  }

  bool enable_ysql() override {
    // Do not create the transaction status table.
    return false;
  }

  Result<uint32_t> GetLoadOnTserver(ExternalTabletServer* server) {
    auto proxy = VERIFY_RESULT(GetMasterLeaderProxy());
    master::GetTableLocationsRequestPB req;
    req.mutable_table()->set_table_name(table_name().table_name());
    req.mutable_table()->mutable_namespace_()->set_name(table_name().namespace_name());
    master::GetTableLocationsResponsePB resp;

    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromMilliseconds(kDefaultTimeoutMillis));
        RETURN_NOT_OK(proxy->GetTableLocations(req, &resp, &rpc));

    uint32_t count = 0;
    std::vector<string> replicas;
    for (const auto& loc : resp.tablet_locations()) {
      for (const auto& replica : loc.replicas()) {
        if (replica.ts_info().permanent_uuid() == server->instance_id().permanent_uuid()) {
          replicas.push_back(loc.tablet_id());
          count++;
        }
      }
    }
    LOG(INFO) << Format("For ts $0, tablets are $1 with count $2",
                        server->instance_id().permanent_uuid(), VectorToString(replicas), count);
    return count;
  }

  Result<bool> AreLeadersOnPreferredOnly() {
    master::AreLeadersOnPreferredOnlyRequestPB req;
    master::AreLeadersOnPreferredOnlyResponsePB resp;
    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(kDefaultTimeoutMillis));
    auto proxy = VERIFY_RESULT(GetMasterLeaderProxy());
    RETURN_NOT_OK(proxy->AreLeadersOnPreferredOnly(req, &resp, &rpc));
    return !resp.has_error();
  }

  Result<std::shared_ptr<master::MasterServiceProxy>> GetMasterLeaderProxy() {
    int idx;
    RETURN_NOT_OK(external_mini_cluster()->GetLeaderMasterIndex(&idx));
    return external_mini_cluster()->master_proxy(idx);
  }

  void CustomizeExternalMiniCluster(ExternalMiniClusterOptions* opts) override {
    opts->extra_tserver_flags.push_back("--placement_cloud=c");
    opts->extra_tserver_flags.push_back("--placement_region=r");
    opts->extra_tserver_flags.push_back("--placement_zone=z${index}");
    opts->extra_master_flags.push_back("--load_balancer_skip_leader_as_remove_victim=false");
  }

  std::unique_ptr<tools::enterprise::ClusterAdminClient> yb_admin_client_;
};

TEST_F(LoadBalancerTest, PreferredZoneAddNode) {
  ASSERT_OK(yb_admin_client_->ModifyPlacementInfo("c.r.z0,c.r.z1,c.r.z2", 3, ""));
  ASSERT_OK(yb_admin_client_->SetPreferredZones({"c.r.z1"}));

  ASSERT_OK(WaitFor([&]() {
    return AreLeadersOnPreferredOnly();
  }, MonoDelta::FromMilliseconds(kDefaultTimeoutMillis), "AreLeadersOnPreferredOnly"));

  std::vector<std::string> extra_opts;
  extra_opts.push_back("--placement_cloud=c");
  extra_opts.push_back("--placement_region=r");
  extra_opts.push_back("--placement_zone=z1");
  ASSERT_OK(external_mini_cluster()->AddTabletServer(true, extra_opts));

  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    return client_->IsLoadBalanced(num_tablet_servers() + 1);
  },  MonoDelta::FromMilliseconds(kDefaultTimeoutMillis * 2), "IsLoadBalanced"));

  auto firstLoad = ASSERT_RESULT(GetLoadOnTserver(external_mini_cluster()->tablet_server(1)));
  auto secondLoad = ASSERT_RESULT(GetLoadOnTserver(external_mini_cluster()->tablet_server(3)));
  // Now assert that both tablet servers in zone z1 have the same count.
  ASSERT_EQ(firstLoad, secondLoad);
}

// Test load balancer idle / active:
// 1. Add tserver.
// 2. Check that load balancer becomes active and completes balancing load.
// 3. Delete table should not activate the load balancer. Not triggered through LB.
TEST_F(LoadBalancerTest, IsLoadBalancerIdle) {
  ASSERT_OK(yb_admin_client_->ModifyPlacementInfo("c.r.z0,c.r.z1,c.r.z2", 3, ""));

  std::vector<std::string> extra_opts;
  extra_opts.push_back("--placement_cloud=c");
  extra_opts.push_back("--placement_region=r");
  extra_opts.push_back("--placement_zone=z1");
  ASSERT_OK(external_mini_cluster()->AddTabletServer(true, extra_opts));
  ASSERT_OK(external_mini_cluster()->WaitForTabletServerCount(num_tablet_servers() + 1,
      MonoDelta::FromMilliseconds(kDefaultTimeoutMillis)));

  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    bool is_idle = VERIFY_RESULT(client_->IsLoadBalancerIdle());
    return !is_idle;
  },  MonoDelta::FromMilliseconds(kDefaultTimeoutMillis * 2), "IsLoadBalancerActive"));

  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    return client_->IsLoadBalancerIdle();
  },  MonoDelta::FromMilliseconds(kDefaultTimeoutMillis * 2), "IsLoadBalancerIdle"));

  YBTableTestBase::DeleteTable();
  // Assert that this times out.
  ASSERT_NOK(WaitFor([&]() -> Result<bool> {
    bool is_idle = VERIFY_RESULT(client_->IsLoadBalancerIdle());
    return !is_idle;
  },  MonoDelta::FromMilliseconds(10000), "IsLoadBalancerActive"));
}

} // namespace integration_tests
} // namespace yb

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

#include <set>
#include <string>

#include <process/defer.hpp>
#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/id.hpp>
#include "process/logging.hpp"
#include <process/process.hpp>

#include <stout/foreach.hpp>
#include <stout/lambda.hpp>
#include <stout/json.hpp>

#include "zookeeper/detector.hpp"
#include "zookeeper/group.hpp"

using namespace process;

using std::set;
using std::string;

namespace zookeeper {

    class LeaderDetectorProcess : public Process<LeaderDetectorProcess> {
    public:
        explicit LeaderDetectorProcess(Group *group);

        // TODO add role/addInfo to the detector
        explicit LeaderDetectorProcess(Group *group, int role, string addInfo);

        virtual ~LeaderDetectorProcess();

        virtual void initialize();

        // LeaderDetector implementation.
        Future<Option<Group::Membership> > detect(
                const Option<Group::Membership> &previous);

    private:
        // Helper that sets up the watch on the group.
        void watch(const set<Group::Membership> &expected);

        // Invoked when the group memberships have changed.
        void watched(const Future<set<Group::Membership> > &memberships);

        Group *group;
        Option<Group::Membership> leader;
        set<Promise<Option<Group::Membership> > *> promises;

        // Potential non-retryable error.
        Option<Error> error;

        // TODO add additional info for maintaining multiple masters and load balancing
        // 1 for master; 2 for slave; 3 for scheduler
        int role;
        // address and other info (starved?) for master; resource, isFree? for slave; resource expectation? for scheduler
        string addInfo;
    };


    // TODO add role to the detector
    LeaderDetectorProcess::LeaderDetectorProcess(Group *_group, int _role, string _addInfo)
            : ProcessBase(ID::generate("leader-detector")),
              group(_group),
              leader(None()),
              role(_role),
              addInfo(_addInfo) { }

    LeaderDetectorProcess::LeaderDetectorProcess(Group *_group)
            : ProcessBase(ID::generate("leader-detector")),
              group(_group),
              leader(None()) { }


    LeaderDetectorProcess::~LeaderDetectorProcess() {
        foreach(Promise < Option<Group::Membership> > *promise, promises)
        {
            promise->future().discard();
            delete promise;
        }
        promises.clear();
    }


    void LeaderDetectorProcess::initialize() {
        watch(set<Group::Membership>());
    }


    Future<Option<Group::Membership> > LeaderDetectorProcess::detect(
            const Option<Group::Membership> &previous) {
        // Return immediately if the detector is no longer operational due
        // to the non-retryable error.
        if (error.isSome()) {
            return Failure(error.get().message);
        }

        // Return immediately if the incumbent leader is different from the
        // expected.
        if (leader != previous) {
            return leader;
        }

        // Otherwise wait for the next election result.
        Promise < Option<Group::Membership> > *promise =
                new Promise <Option<Group::Membership>>();
        promises.insert(promise);
        return promise->future();
    }


    void LeaderDetectorProcess::watch(const set<Group::Membership> &expected) {
        group->watch(expected)
                .onAny(defer(self(), &Self::watched, lambda::_1));
    }


    void LeaderDetectorProcess::watched(
            const Future<set<Group::Membership> > &memberships) {
        CHECK(!memberships.isDiscarded());

        if (memberships.isFailed()) {
            LOG(ERROR) << "Failed to watch memberships: " << memberships.failure();

            // Setting this error stops the watch loop and the detector
            // transitions to an erroneous state. Further calls to detect()
            // will directly fail as a result.
            error = Error(memberships.failure());
            leader = None();
            foreach(Promise < Option<Group::Membership> > *promise, promises)
            {
                promise->fail(memberships.failure());
                delete promise;
            }
            promises.clear();
            return;
        }

        // Update leader status based on memberships.
        if (leader.isSome() && memberships.get().count(leader.get()) == 0) {
            VLOG(1) << "The current leader (id=" << leader.get().id() << ") is lost";
        }

        // Run an "election". The leader is the oldest member (smallest
        // membership id). We do not fulfill any of our promises if the
        // incumbent wins the election.
        // TODO detect two leaders instead of one
        Option<Group::Membership> current;
        Option<Group::Membership> current2;
        foreach(
        const Group::Membership &membership, memberships.get()) {
            if (current != min(current, membership)) {
                current2 = current;
                current = membership;
            } else {
                current2 = min(current2, membership);
            }
//            current = min(current, membership);
        }

        if (role == 1) {
            if (current2.isSome()) {
                // TODO for master
                string data = group->data(current.get()).get().get();
//                LOG(INFO) << "In detector see current data: " << data;
//                data = group->data(current2.get()).get().get();
//                LOG(INFO) << "In detector see current2 data: " << data;
                JSON::Object object = JSON::parse<JSON::Object>(data).get();
                string pidPath = "pid";
                string pid = stringify(object.values[pidPath]);
                string addr = pid.substr(8, pid.size() - 9);
                LOG(INFO) << "In detector see addr: " << addr;
                if (addr != addInfo) {
                    LOG(INFO) << "Addr " << addr << " not equals to " << addInfo;
                    data = group->data(current2.get()).get().get();
                    object = JSON::parse<JSON::Object>(data).get();
                    pid = stringify(object.values[pidPath]);
                    addr = pid.substr(8, pid.size() - 9);
                    LOG(INFO) << "In detector see addr: " << addr;
                    if (addr == addInfo) {
                        LOG(INFO) << "Addr " << addr << " equals to " << addInfo;
                        current = current2;
                    }
                }
            }
            if (current != leader) {
//                if (current.isSome()) {
//                    string data = group->data(current.get()).get().get();
//                    JSON::Object object = JSON::parse<JSON::Object>(data).get();
//                    string pidPath = "pid";
//                    string pid = stringify(object.values[pidPath]);
//                    LOG(INFO) << "In detector see current pid: " << pid;
//                }
                LOG(INFO) << "Detected a new leader: "
                << (current.isSome()
                    ? "(id='" + stringify(current.get().id()) + "')"
                    : "None");

                foreach(Promise < Option<Group::Membership> > *promise, promises)
                {
                    promise->set(current);
                    delete promise;
                }
                promises.clear();
                // TODO assign new leader
                leader = current;
            }
        } else if (current != leader &&
                   ((leader.isSome() && current2 != leader) || leader.isNone())) {

            // TODO need to modify the logic for load balancing
            if (current2.isSome()) {
                LOG(INFO) << "Detected two leaders!";
                current = current2;
//                current = (time(0) % 2 == 0) ? current : current2;
            }

            LOG(INFO) << "Detected a new leader: "
            << (current.isSome()
                ? "(id='" + stringify(current.get().id()) + "')"
                : "None");

            foreach(Promise < Option<Group::Membership> > *promise, promises)
            {
                promise->set(current);
                delete promise;
            }
            promises.clear();
            // TODO assign new leader
            leader = current;
        }

//        leader = current;
        watch(memberships.get());
    }

    // TODO for master, add self address to the constructor for leader detection
    LeaderDetector::LeaderDetector(Group *group, int role, string addInfo) {
        process = new LeaderDetectorProcess(group, role, addInfo);
        spawn(process);
    }

    LeaderDetector::LeaderDetector(Group *group) {
        process = new LeaderDetectorProcess(group);
        spawn(process);
    }


    LeaderDetector::~LeaderDetector() {
        terminate(process);
        process::wait(process);
        delete process;
    }


    Future<Option<Group::Membership> > LeaderDetector::detect(
            const Option<Group::Membership> &membership) {
        return dispatch(process, &LeaderDetectorProcess::detect, membership);
    }

} // namespace zookeeper {

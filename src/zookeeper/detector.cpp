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
#include <sstream>
#include <string>
#include <vector>
#include <map>

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

#define NUM_OF_LEADERS 3;

using namespace process;

using std::set;
using std::string;
using std::vector;
using std::map;
using std::stringstream;

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

        // single leader election
        void singleLeaderElection(const Future<set<Group::Membership> > &memberships);

        // multiple-leader election
        void multiLeaderElection(const Future<set<Group::Membership> > &memberships);

        Group *group;
        Option<Group::Membership> leader;
        set<Promise<Option<Group::Membership> > *> promises;

        // Potential non-retryable error.
        Option<Error> error;

        // TODO add additional info for maintaining multiple masters and load balancing
        // 1 for master; 2 for slave; 3 for scheduler
        int role;

        // address and other info (starved?) for master; resource, isFree? for slave; resource expectation? for scheduler
        string localAddress;
        int numOfLeaders;
        string lbStrategy;
        // for seen masters' address
        map<int32_t, string> addrMap;
    };


    // TODO add role to the detector
    LeaderDetectorProcess::LeaderDetectorProcess(Group *_group, int _role, string _addInfo)
            : ProcessBase(ID::generate("leader-detector")),
              group(_group),
              leader(None()),
              role(_role) {
        if (role == 3) {
            numOfLeaders = NUM_OF_LEADERS;
            lbStrategy = "R";

        } else {
            vector<string> infoVector;
            stringstream sStream(_addInfo);
            string info;
            while (std::getline(sStream, info, ' ')) {
                infoVector.push_back(info);
            }
            if (infoVector.size() == 3) {
                localAddress = infoVector[0];
                numOfLeaders = atoi(infoVector[1].c_str());
                lbStrategy = infoVector[2];
            }
        }

    }

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

    void LeaderDetectorProcess::singleLeaderElection(
            const Future<set<Group::Membership> > &memberships) {
        // Run an "election". The leader is the oldest member (smallest
        // membership id). We do not fulfill any of our promises if the
        // incumbent wins the election.
        Option<Group::Membership> current;
        foreach(
        const Group::Membership &membership, memberships.get()) {
            current = min(current, membership);
        }

        if (current != leader) {
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
        }

        leader = current;
        watch(memberships.get());
    }

    void LeaderDetectorProcess::multiLeaderElection(
            const Future<set<Group::Membership> > &memberships) {
        // Run an "election" for multiple leaders in the MESOS.
        // The leaders is the oldest members (smallest
        // membership ids). We do not fulfill any of our promises if the
        // incumbent wins the election.
        // TODO detect two leaders instead of one
        vector<Option<Group::Membership>> currents(numOfLeaders);
        Option<Group::Membership> current;
        foreach(
        const Group::Membership &membership, memberships.get()) {
            int maxInd = 0;
            if (currents[0].isNone()) {
                currents[0] = membership;
            } else {
                for (int i = 1; i < numOfLeaders; i++) {
                    if (currents[i].isNone()) {
                        maxInd = i;
                        break;
                    } else if (currents[i].get().id() > currents[maxInd].get().id()) {
                        maxInd = i;
                    }
                }
            }
            if (currents[maxInd].isNone() || membership.id() < currents[maxInd].get().id()) {
                currents[maxInd] = membership;
            }
        }

        // make the smallest candidate as the first element
        if (currents[0].isSome()) {
            int minInd = 0;
            for (int i = 1; i < numOfLeaders; i++) {
                if (currents[i].isNone()) {
                    break;
                } else if (currents[i].get().id() < currents[minInd].get().id()) {
                    minInd = i;
                }
            }
            if (minInd != 0) {
                Option<Group::Membership> tmp = currents[0];
                currents[0] = currents[minInd];
                currents[minInd] = tmp;
            }
        }

        if (role == 1) {
            // select local leader for master
            // if the current master is one of the global leaders, it is the local leader
            // else it will be a backup master and local leader will be one of the global leaders
            current = currents[0];
            for (int i = 0; i < numOfLeaders; i++) {
                if (!currents[i].isNone()) {
                    // first see if saw this membership before
                    if (addrMap.find(currents[i].get().id()) != addrMap.end()) {
                        if (addrMap[currents[i].get().id()] == localAddress) {
                            current = currents[i];
                            break;
                        }
                    } else {
                        string data = group->data(currents[i].get()).get().get();
                        JSON::Object object = JSON::parse<JSON::Object>(data).get();
                        string pidPath = "pid";
                        string pid = stringify(object.values[pidPath]);
                        string addr = pid.substr(8, pid.size() - 9);
                        LOG(INFO) << "In detector see addr: " << addr;
                        addrMap[currents[i].get().id()] = addr;
                        if (addr == localAddress) {
                            // if the current master is one of the leaders, assign itself as the local leader
                            current = currents[i];
                            break;
                        }
                    }
                }
            }

            if (current != leader) {
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
                // assign new leader
                leader = current;
            }
        } else {
            // select local leader for slave/scheduler
            // if previous local leader is among the global leaders, nothing changes
            // else the new local leader will be chosed based on the given load balancing strategy

            // first check if the previous local leader is still one of the global leaders
            bool preLeaderFound = false;
            int numOfL = 0;
            for (int i = 0; i < numOfLeaders; i++) {
                if (currents[i] == leader) {
                    preLeaderFound = true;
                    break;
                }
                if (currents[i].isNone()) {
                    break;
                } else {
                    numOfL++;
                }
            }

            if (!preLeaderFound) {
                char numStr[21];
                sprintf(numStr, "%d", numOfL);
                LOG(INFO) << "Detected leaders: " << numStr;
                if (lbStrategy == "R") {
                    // random master assignment
                    current = currents[rand() % numOfL];
                } else if (lbStrategy == "LRU") {
                    // LRU master assignment
                    current = currents[0];

                    if (numOfL > 0) {
                        string curr_data = group->data(current.get()).get().get();
                        JSON::Object curr_object = JSON::parse<JSON::Object>(curr_data).get();

                        for (int i = 0; i < numOfL; i++) {

                            // try to get timestamp from the master's znode data
                            string temp_data = group->data(currents[i].get()).get().get();
                            JSON::Object temp_object = JSON::parse<JSON::Object>(temp_data).get();

                            if (temp_object.values.find("updated") != temp_object.values.end()) {
                                if (stringify(temp_object.values["updated"]) <
                                    stringify(curr_object.values["updated"])) {
                                    current = currents[i];
                                    curr_data = group->data(current.get()).get().get();
                                    curr_object = JSON::parse<JSON::Object>(curr_data).get();
                                }
                            } else {
                                current = currents[i];
                                break;
                            }
                        }

                    }

                    if (current.isSome()) {
                        string data_str = group->data(current.get()).get().get();
                        JSON::Object object_json = JSON::parse<JSON::Object>(data_str).get();

                        object_json.values["updated"] = static_cast<long int> (time(0));

                        string data = stringify(object_json);

                        bool result = group->post(current.get(), data).get();

                        if (result) {
                            LOG(INFO) << "SUCCESS! Update a leader: " << "(id='" << stringify(current.get().id()) <<
                            ")";
                        } else {
                            LOG(INFO) << "FAIL! Update a leader: " << "(id='" << stringify(current.get().id()) << ")";
                        }

                    }
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
        }
        watch(memberships.get());
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

        if (role == 0) {
            // original leader election algorithm
            singleLeaderElection(memberships);
        } else {
            // if the role is set, then perform multiple leader election
            multiLeaderElection(memberships);
        }
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

use std::collections::BTreeSet;
use std::fmt::Debug;

use stateright::actor::Id;

use crate::stable_set::{Member, StableSet};
use crate::{build_msg, ELDER_COUNT};

pub type Elders = BTreeSet<Id>;

#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub enum Msg {
    ReqJoin(Id),
    ReqLeave(Id),
    JoinShare(Member),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Membership {
    pub stable_set: StableSet,
}

impl Membership {
    pub fn new(genesis: &BTreeSet<Id>) -> Self {
        let mut stable_set = StableSet::default();

        for genesis_id in genesis.iter().copied() {
            let genesis_member = Member {
                id: genesis_id,
                ord_idx: 0,
            };
            for other_genesis_id in genesis.iter().copied() {
                stable_set.add(genesis_member.clone(), other_genesis_id);
            }
        }

        stable_set.process_ready_actions(genesis);

        assert_eq!(&BTreeSet::from_iter(stable_set.ids()), genesis);

        Self { stable_set }
    }

    fn build_msg(&self, msg: Msg) -> crate::Msg {
        build_msg(self, msg)
    }

    pub fn req_join(&self, id: Id) -> crate::Msg {
        self.build_msg(Msg::ReqJoin(id))
    }

    pub fn req_leave(&mut self, id: Id) -> crate::Msg {
        if let Some(member) = self.stable_set.member_by_id(id) {
            self.handle_leave_share(id, member, id);
        }
        self.build_msg(Msg::ReqLeave(id))
    }

    pub fn is_member(&self, id: Id) -> bool {
        self.stable_set.contains(id)
    }

    pub fn members(&self) -> BTreeSet<Member> {
        self.stable_set.members()
    }

    pub fn elders(&self) -> Elders {
        BTreeSet::from_iter(self.members().into_iter().take(ELDER_COUNT).map(|m| m.id))
    }

    pub fn merge(&mut self, stable_set: StableSet, id: Id, src: Id) -> BTreeSet<Id> {
        let mut additional_members_to_sync = BTreeSet::new();

        for member in stable_set.members() {
            let m_id = member.id;

            if self.handle_join_share(id, member, src) {
                additional_members_to_sync.insert(m_id);
                additional_members_to_sync.extend(self.elders());
            }
        }

        for member in stable_set.joining() {
            let m_id = member.id;
            if self.handle_join_share(id, member, src) {
                additional_members_to_sync.insert(m_id);
                additional_members_to_sync.extend(self.elders());
            }
        }

        for member in stable_set.leaving() {
            let m_id = member.id;
            if self.handle_leave_share(id, member, src) {
                additional_members_to_sync.insert(m_id);
                additional_members_to_sync.extend(self.elders());
            }
        }

        // For each member we know is leaving, check if the other node has already removed it.
        let to_handle = Vec::from_iter(
            self.stable_set
                .leaving()
                .filter(|m| !stable_set.is_member(m)),
        );
        for member in to_handle {
            let m_id = member.id;
            if self.handle_leave_share(id, member, src) {
                additional_members_to_sync.insert(m_id);
                additional_members_to_sync.extend(self.elders());
            }
        }

        additional_members_to_sync
    }

    pub fn on_msg(&mut self, elders: &BTreeSet<Id>, id: Id, src: Id, msg: Msg) -> BTreeSet<Id> {
        let mut additional_members_to_sync = BTreeSet::new();
        match msg {
            Msg::ReqJoin(candidate_id) => {
                if self.stable_set.member_by_id(candidate_id).is_none() && elders.contains(&id) {
                    let latest_ord_idx = self
                        .stable_set
                        .members()
                        .iter()
                        .map(|m| m.ord_idx)
                        .max()
                        .unwrap_or(0);
                    let ord_idx = latest_ord_idx + 1;

                    let member = Member {
                        id: candidate_id,
                        ord_idx,
                    };

                    if self.handle_join_share(id, member, id) {
                        additional_members_to_sync.insert(candidate_id);
                        additional_members_to_sync.extend(elders);
                    }
                }
            }
            Msg::ReqLeave(to_remove) => {
                if let Some(member) = self.stable_set.member_by_id(to_remove) {
                    if self.handle_leave_share(id, member, src) {
                        additional_members_to_sync.insert(to_remove);
                        additional_members_to_sync.extend(elders);
                    }
                }
            }
            Msg::JoinShare(member) => {
                let m_id = member.id;
                if self.handle_join_share(id, member, src) {
                    additional_members_to_sync.insert(m_id);
                    additional_members_to_sync.extend(elders);
                }
            }
        }
        additional_members_to_sync
    }

    pub fn process_pending_actions(&mut self, id: Id) -> BTreeSet<Id> {
        let stable_set_changed = self.stable_set.process_ready_actions(&self.elders());

        if stable_set_changed && self.elders().contains(&id) {
            self.stable_set.ids().filter(|e| e != &id).collect()
        } else {
            Default::default()
        }
    }

    fn handle_join_share(&mut self, id: Id, member: Member, witness: Id) -> bool {
        if self.stable_set.is_member(&member) {
            return false;
        }

        let first_time_seeing_join = self.stable_set.add(member.clone(), witness);
        self.stable_set.add(member, id);

        first_time_seeing_join
    }

    fn handle_leave_share(&mut self, id: Id, member: Member, witness: Id) -> bool {
        if !self.stable_set.is_member(&member) {
            return false;
        }

        let first_time_seeing_leave = self.stable_set.remove(member.clone(), witness);
        self.stable_set.remove(member, id);

        first_time_seeing_leave
    }
}

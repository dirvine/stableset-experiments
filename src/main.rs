// main.rs
mod fake_crypto;
mod ledger;
mod membership;
mod stable_set;

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

use fake_crypto::majority;
use ledger::{genesis_dbc, Tx, Wallet};
use membership::Membership;
use stable_set::StableSet;
use stateright::{
    actor::{Actor, ActorModel, ActorModelState, Id, Network, Out},
    Expectation, Model,
};

const ELDER_COUNT: usize = 4;

pub fn build_msg(membership: &Membership, action: impl Into<Action>) -> Msg {
    let mut stable_set = membership.stable_set.clone();

    for (_, witnesses) in stable_set.joining_members.iter_mut() {
        witnesses.clear()
    }

    for (_, witnesses) in stable_set.leaving_members.iter_mut() {
        witnesses.clear()
    }

    Msg {
        stable_set,
        action: action.into(),
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct State {
    pub membership: Membership,
    is_leaving: bool,
    pub wallet: Wallet,
}

impl State {
    fn elders(&self) -> BTreeSet<Id> {
        self.membership.elders()
    }

    fn build_msg(&self, action: Action) -> Msg {
        build_msg(&self.membership, action)
    }
}

#[derive(Clone)]
pub struct Node {
    pub genesis_nodes: BTreeSet<Id>,
    pub peers: Vec<Id>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct Msg {
    stable_set: StableSet,
    action: Action,
}

impl Debug for Msg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Msg({:?}, {:?})", self.stable_set, self.action)
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub enum Action {
    Membership(membership::Msg),
    Wallet(ledger::Msg),
    Sync,
    StartReissue,
    TriggerLeave,
}

impl Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Membership(m) => write!(f, "{m:?}"),
            Self::Wallet(m) => write!(f, "{m:?}"),
            Self::Sync => write!(f, "Sync"),
            Self::StartReissue => write!(f, "StartReissue"),
            Self::TriggerLeave => write!(f, "TriggerLeave"),
        }
    }
}

impl From<membership::Msg> for Action {
    fn from(msg: membership::Msg) -> Self {
        Self::Membership(msg)
    }
}

impl From<ledger::Msg> for Action {
    fn from(msg: ledger::Msg) -> Self {
        Self::Wallet(msg)
    }
}

impl Actor for Node {
    type Msg = Msg;
    type State = State;

    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        let membership = Membership::new(&self.genesis_nodes);
        let wallet = Wallet::new(&self.genesis_nodes);

        let state = State {
            membership,
            wallet,
            is_leaving: false,
        };

        if !self.genesis_nodes.contains(&id) {
            o.broadcast(&self.genesis_nodes, &state.membership.req_join(id));
        }

        // if id > Id::from(self.peers.len().saturating_sub(2)) {
        // First two nodes will try to spend the genesis
        o.send(id, state.build_msg(Action::StartReissue));
        // }

        state
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        let elders = state.elders();
        let Msg { stable_set, action } = msg;

        let mut nodes_to_sync = state.to_mut().membership.merge(stable_set, id, src);

        match action {
            Action::Sync => (),
            Action::Membership(msg) => {
                nodes_to_sync.extend(state.to_mut().membership.on_msg(&elders, id, src, msg));
            }
            Action::Wallet(msg) => {
                let membership = state.membership.clone();
                state.to_mut().wallet.on_msg(&membership, id, src, msg, o)
            }
            Action::StartReissue => {
                let input = genesis_dbc();

                let reissue_amount =
                    (0..self.peers.len()).find(|x| Id::from(*x) == id).unwrap() as u64;
                let difference = input.amount() - reissue_amount;

                let membership = state.membership.clone();
                state.to_mut().wallet.reissue(
                    &membership,
                    vec![input],
                    vec![reissue_amount, difference],
                    o,
                );
            }
            Action::TriggerLeave => {
                o.broadcast(&elders, &state.to_mut().membership.req_leave(id));
            }
        }
        if id > Id::from((self.peers.len() * 2) / 3)
            && state.membership.is_member(id)
            && !state.is_leaving
        {
            state.to_mut().is_leaving = true;
            o.send(id, state.build_msg(Action::TriggerLeave));
        }

        nodes_to_sync.extend(state.to_mut().membership.process_pending_actions(id));
        nodes_to_sync.remove(&id);

        o.broadcast(&nodes_to_sync, &state.build_msg(Action::Sync))
    }
}

#[derive(Clone)]
struct ModelCfg {
    elder_count: usize,
    server_count: usize,
    network: Network<<Node as Actor>::Msg>,
}

fn reference_stable_set(state: &ActorModelState<Node, Vec<Msg>>) -> StableSet {
    state
        .actor_states
        .iter()
        .find(|s| !s.is_leaving)
        .map(|s| s.membership.stable_set.clone())
        .unwrap_or_default()
}

fn prop_stable_set_converged(state: &ActorModelState<Node, Vec<Msg>>) -> bool {
    let reference_members = reference_stable_set(state).members();

    state
        .actor_states
        .iter()
        .filter(|s| !s.is_leaving)
        .all(|actor| actor.membership.stable_set.members() == reference_members)
}

fn prop_all_nodes_joined_who_havent_left(state: &ActorModelState<Node, Vec<Msg>>) -> bool {
    let reference_stable_set = reference_stable_set(state);
    state
        .actor_states
        .iter()
        .enumerate()
        .filter(|(_, actor)| !actor.is_leaving)
        .all(|(id, _actor)| reference_stable_set.contains(id.into()))
}

fn prop_all_nodes_who_are_leaving_eventually_left(state: &ActorModelState<Node, Vec<Msg>>) -> bool {
    let reference_stable_set = reference_stable_set(state);

    state
        .actor_states
        .iter()
        .enumerate()
        .filter(|(_, actor)| actor.is_leaving)
        .all(|(id, _)| !reference_stable_set.contains(id.into()))
}

#[allow(unused)]
fn prop_unspent_outputs_equals_genesis_amount(state: &ActorModelState<Node, Vec<Msg>>) -> bool {
    state
        .actor_states
        .iter()
        .all(|actor| genesis_dbc().amount() == actor.wallet.ledger.sum_unspent_outputs())
}

fn prop_no_double_spends(state: &ActorModelState<Node, Vec<Msg>>) -> bool {
    let actor_by_id = BTreeMap::from_iter(
        state
            .actor_states
            .iter()
            .enumerate()
            .map(|(id, s)| (Id::from(id), s)),
    );

    let concurrent_txs = BTreeSet::from_iter(state.actor_states.iter().flat_map(|a| {
        let mut transactions: BTreeMap<Tx, usize> = Default::default();

        let elders = a.membership.elders();

        for elder in &elders {
            if let Some(tx) = actor_by_id
                .get(elder)
                .unwrap()
                .wallet
                .read_tx(&genesis_dbc().id())
            {
                let tx_count = transactions.entry(tx).or_default();
                *tx_count += 1;
            }
        }

        transactions
            .into_iter()
            .filter(move |(_, count)| majority(*count, elders.len()))
            .map(|(tx, _)| tx)
    }));

    concurrent_txs.len() <= 1
}

impl ModelCfg {
    fn into_model(self) -> ActorModel<Node, Self, Vec<Msg>> {
        ActorModel::new(self.clone(), vec![])
            .actors((0..self.server_count).map(|_i| Node {
                genesis_nodes: BTreeSet::from_iter((0..self.elder_count).into_iter().map(Id::from)),
                peers: (0..self.server_count).map(Id::from).collect(),
            }))
            .init_network(self.network)
            .property(
                Expectation::Eventually,
                "everyone who hasn't left converges on the same stable set",
                |_, state| prop_stable_set_converged(state),
            )
            .property(
                Expectation::Eventually,
                "everyone who hasn't left is part of the final stable set",
                |_, state| prop_all_nodes_joined_who_havent_left(state),
            )
            .property(
                Expectation::Eventually,
                "everyone who started leaving, will leave",
                |_, state| prop_all_nodes_who_are_leaving_eventually_left(state),
            )
            .property(Expectation::Always, "Ledger balances", |_, state| {
                prop_unspent_outputs_equals_genesis_amount(state)
            })
            .property(
                Expectation::Always,
                "Never two nodes aggregate a double spend",
                |_, state| prop_no_double_spends(state),
            )
    }
}

fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let network = Network::new_unordered_nonduplicating([]);

    ModelCfg {
        elder_count: 1,
        server_count: 5,
        network,
    }
    .into_model()
    .checker()
    .threads(num_cpus::get())
    .serve("localhost:3000");
}

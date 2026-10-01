# ADR 0005: Fixed membership

## Context

A Raft cluster's safety rests on any two quorums overlapping. If nodes
disagree about who the members are, they can disagree about what a quorum is,
and two disjoint groups can each believe they have one. Changing membership
safely therefore has to go through the log itself (joint consensus, or
single-server changes), with rules about when a new configuration takes
effect, how a new member catches up before it can vote, and what a removed
leader does.

## Decision

The set of voters is fixed in configuration. Every node is started with the
same `--peers` list, and that list never changes while the cluster holds
data. The quorum is `len(peers)/2 + 1`, computed from the configured list and
never from the number of nodes that happen to be reachable.

Three nodes is the default. Five is supported and tested.

Three checks keep a misconfigured node from doing harm:

- A node's data directory records its node ID and cluster ID on first start
  and refuses to open under a different one.
- Every peer message carries the cluster ID, sender and recipient. A message
  from another cluster, from a node not in the list, or addressed to someone
  else is refused before it reaches Raft.
- In secure mode the sender named in a message must be the identity in the
  TLS certificate that carried it.

## Consequences

Good:

- A whole class of subtle bugs is out of scope, and the quorum arithmetic is
  one line.
- Tests can state exactly which majorities exist.

Bad, and these are real operational limits:

- **You cannot add or remove a node.** Growing from three to five means
  building a new cluster and copying the data.
- **You cannot replace a node that has lost its disk.** This one is easy to
  get wrong, so to be explicit: starting a node with the old ID and an empty
  data directory is **unsafe**. The node has forgotten what it voted for and
  which entries it acknowledged. If it had acknowledged an entry that only
  it and the leader held, and the leader then fails, the emptied node can
  help elect a leader that never saw that entry, and a committed write is
  gone. The supported response to a lost disk is to keep running on the
  remaining majority and rebuild the cluster.
- Changing a node's address is fine (the list maps IDs to addresses; restart
  every node with the new list). Changing its ID is not.

## Alternatives considered

- **Single-server membership changes** as described in the Raft
  dissertation. The right next feature, and the one that makes disk
  replacement possible. Listed as future work, to be done only with the same
  simulation and history-checking coverage the rest of the core has.
- **Reducing the quorum when nodes are unreachable.** Never: that is how
  two halves of a partition both accept writes.

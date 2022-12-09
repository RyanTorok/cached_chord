**  Load-balanced Distributed Hash Tables Benefit from Caching

A *distributed hash table* (DHT) is a system that allows clients to look up entries in a large
key-value store, where the data is sharded across many different machines. A well-known example of a
DHT your computer uses every day is the [[https:en.wikipedia.org/wiki/Domain_Name_System][Domain Name System]] (DNS), which allows your computer to
translate from a URL to an IP address. When you told your browser to visit this article on
medium.com, the browser internally pings the DNS to know where to send its HTTP request.

The DNS is organized using a hierarchical structure. When your browser requested the IP address for
medium.com, it first pings a root DNS server to learn the address of another server responsible for
all .com websites, and that server redirects you to the medium.com server. For websites like
[[https:princeton.edu][princeton.edu]] that have multiple subdomains (e.g. [[https:cs.princeton.edu][cs.princeton.edu]]), the DNS resolution process can
take 3-4 redirects, which is very slow. To speed up this process, the DNS aggressively caches
resolutions of common websites at local DNS servers that are physically close to the client. In
practice, this cuts the number of root DNS server accesses for a typical user down to about one per
day, rather than hundreds.

However, even with aggressive caching, the DNS still relies on the top levels of the hierarchy to be
large, beefy server clusters that can handle billions of requests per day. Meanwhile, there are
other settings that require distributed hash tables where this is just not feasible. For example,
peer-to-peer file sharing services like [[https:www.bittorrent.com][BitTorrent]] and [[https:ipfs.tech][IPFS]] rely on DHTs to allow clients to find
the location of a file stored in their global network. However, these services are decentralized by
design and cannot expect any machine to have the compute power to act as the root of a hierarchical
DHT. This issue is addressed using load-balanced DHTs, such as Chord.


**  What is Chord?

*Chord* ([[[[[[https:www.cs.princeton.edu/courses/archive/fall22/cos418/papers/chord.pdf][Stoica et al. 2001]]) is a distributed hash table designed to spread its request load evenly across
many nodes. Rather than having a single root node that handles the top level of every request, Chord
organizes its nodes in a ring, where each node is responsible for a subset of the key-value pairs:

[[file:~/chord_basic.png]]

In this example, we have a DHT that has nodes with IDs 0, 1, 3, 5, and 6 that can store the keys
0-7. Each key is stored at the node with the next-higher ID, modulo the number of IDs. For example,
node 3 stores keys 2 and 3, and node 0 stores keys 7 and 0. Each node also stores the identity of
its successor and predecessor, so it can redirect clients who access other keys.

To look up a key, a client can make a request to any of the nodes. If that node is responsible for
the key, it simply returns the associated value. However if a different node is responsible for that
key, the node must determine the identity of the node that is responsible.


**  Finger Tables

Our node could forward the request on using its successor and predecessor pointers, but if that is
all we had, this process would be horribly slow. In the worst case, we would have to go halfway
around the ring to find the correct key, and make n / 2 network lookup requests if our ring has n
nodes.

One solution to this problem is to have every node store the identity of every other node and their
associated key range. Systems like [[https:en.wikipedia.org/wiki/Amazon_DynamoDB][Dynamo]] intended to run in a datacenter with relatively few nodes
actually do this. However, since Chord desires to power large peer-to-peer systems with many nodes
that can enter and leave the ring as they please, maintaining such a list at every node and keeping
them up to date is extremely tedious and error-prone. 

Instead, Chord scalably speeds up lookups using *finger tables*. In Chord, each node stores the
identities of the nodes responsible for the key 1, 2, 4, 8, 16, etc. away from its own ID. This
trick makes the entire Chord ring act similarly to a skip list rooted at every node, where our
expected number of hops is only logarithmic in the number of nodes. At the same time, it incurs only
a logarithmic storage blowup on each node that is much more practical to maintain.

[[file:~/chord-finger.png]]

Finally, Chord keeps both the successor and finger references up to date as node enter and leave the
ring by periodically sending messages internally to check the identity of the successors and finger
targets.


** Caching in Chord

Unlike hierarchical DHTs that run systems like the DNS that employ aggressive use of caching to make
them practical to deploy, the original implementation of Chord does not use caching of any kind. The
authors of the original paper did not consider it, because the structure of the finger table already
provides good scalability. The authors found experimentally that the lookup latency increases only
logarithmically with the number of nodes in the system.

But what would caching look like in Chord? If we cache the values for popular keys like in a
hierarchical DHT, we end up with a consistency problem. Suppose node A caches [k = 1] from node B, then
another node took a different path to B that does not include A, and then sets [k = 2]. A's value
becomes stale, and this cannot be resolved without doing a consistnecy check with the
original node on every cache hit, defeating the purpose of the cache.

However, we can instead cache the *location* of popular keys. Then, if a node gets a cache hit, it
can skip directly to the correct node and avoid the logarithmic number of redirects to get
there. This strategy is not immune from stale entries either; however, in the uncommon event
they do happen, they only affect the performance of the lookup (extra redirects), rather than let us
return a stale value and break consistency.

If the original authors of the Chord paper tried this, they would have likely concluded there was a
minimal performance gain, because they assumed the distribution of lookup requests was uniform over
the key set. However, in most practical applications, the distribution of requests follows a Zipfian
distribution, where a small subset of keys are exponentially more popular than most. In this
setting, we hypothesized that caching the location of those popular keys will significantly reduce
the expected lookup time by reducing the number of hops to get to the responsible node.


** Implementation

To test this, we desired to reproduce the Chord implementation to perform a side-by-side comparsion
of the read latency when the lookups

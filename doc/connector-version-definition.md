# Connector version release strategy

It's unquestionable, given this documentation to clarify the new connector version mapping between flink and pulsar.
 
Yet, as demonstrated by the wide range of different ideas on how it should be improved, it's hard to find consensus on what 'better' actually looks like. Having looked through the discussions in the slack group, there are two broad categories into which the concerns fall into: the first is helping users choose the proper connector for their project and the second one is reducing the connector maintainer's work.
 
## Better version mapping
 
How to make users choose the right connector version which contains the known issues? We would solve this problem in four ways.
 
### Connector repository would follow the flink style
 
Every flink's main version would have a separate connector branch for releasing the related version. Such as `release-1.13`, `release-1.12`, `master`. The connector repository would have the same branch naming like flink. The remaining releasing branches would be removed when we released the new version. The new released connector would be tagged instead of creating a new branch.
 
The connector’s master branch, just like flink, would be the last snapshot code which contains some untested feature and would be pointing to the latest flink snapshot version.
 
### Semantic connector version mapping between the pulsar and flink
 
The latest flink released version would be a three part version. We only add an extra dot patch version after the flink version as the connector version. eg. If flink released a `1.13.4` version, connector would release a `1.13.4.0` version later. Any bugfix and code modification would bump the patch version. If pulsar have a new released version, we would bump the internal pulsar client version and release a new patch version.
 
Since flink’s API is backward and forward compatible among its mainstream version (like 1.12.1 and 1.12.4). Users could choose the latest connector as long as the first two versions are the same as flink’s version. For example, if the latest connector version for release-1.13 branch is 1.13.3.5, you can use it for any 1.13.x flink.
 
### Simplify connector artifact definition
 
Since the connector marks the flink version in its version naming convention, no need to add the flink version in the connector artifact. All the flink bundled connectors have an extra scala tag for providing right flink dependencies or the compiled classes. We preserve this scala tag only to match the flink style.
 
The new connector artifact would be `pulsar-flink-connector-${scala.binary.version}` and `pulsar-flink-connector-bundled-${scala.binary.version}`.
 
### Pulsar client would be an optional dependency
 
In the new connector development specifications, we add a minimal supported pulsar client version constraint to every connector’s `release-1.xx` branches. That means every big flink version would have a target pulsar version. Any legacy pulsar which below the branch minimal supported version wouldn’t be supported by the related connector. But you can use an old supported connector branch which targets an old flink.
 
We plan to use pulsar `2.5.x` as the minimal supported version for all the maintenance branches. That means some feature on the new pulsar client would have a built-in switch by reading your client version. Every time the connector would release two kinds of packages.
 
1. `pulsar-flink-connector-bundled-${scala.binary.version}`: The connector which bundles the latest pulsar client .
2. `pulsar-flink-connector-${scala.binary.version}`: The connector without a specified pulsar client.
 
## Guidance on code maintaining
 
This is a huge change for the current pulsar connector, a lot of documentation and modification should be listed. I list some guidance for maintaining the connector.
 
### Connector's package architecture should follow flink style.
 
Since this project is a flink target connector for connecting pulsar to flink. Any code style (such as package naming, class naming, branch naming, etc.) should follow the flink style.
 
### Make streamnative-ci support the connector's new release strategy
 
The connector uses a rolling release policy, we should release the connector more frequently and expose the remaining problem early. So the automation on releasing should be triggered more easily. Our streamnative-ci don’t support this release style currently. We should add these abilities to it.
 
### Focus on new flink API
 
The legacy connector (the code which relies on flink old API) should be frozen. No more new features, just bugfix on it. We should start developing the new flink connector and plan the next key feature: uniform batch and streaming connector.

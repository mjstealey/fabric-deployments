# Credits & acknowledgments

`fabric-deployments` stands on the work of several open testbeds, workflow
systems, and observability tools. This project glues them together; the credit
for the underlying technology belongs to the projects below.

## Infrastructure

- **[FABRIC Testbed](https://fabric-testbed.net/)** — the NSF-funded,
  programmable research infrastructure this project provisions onto. Every slice,
  the FABNetv4 networking, and the FABlib (`fabrictestbed-extensions`) runtime
  used here come from FABRIC.

## Workflow & scheduling

- **[Pegasus WMS](https://pegasus.isi.edu/)** — the Workflow Management System
  (developed at USC Information Sciences Institute) that plans and executes the
  scientific workflows run on the provisioned pool.
- **[HTCondor](https://htcondor.org/)** — the high-throughput computing system
  (from the UW–Madison Center for High Throughput Computing) that forms the
  execution pool Pegasus submits into.

## Observability pipeline

- **[Vector](https://vector.dev/)** — the high-performance observability data
  pipeline (by Datadog) that tails workflow-monitor's JSONL output and ships it
  over FABNetv4 to Elasticsearch.
- **[Elasticsearch + Kibana](https://www.elastic.co/)** — the search/analytics
  engine and visualization UI (by Elastic) that receive, index, and display the
  shipped workflow events.
- **[workflow-monitor](https://github.com/pegasus-isi/workflow-monitor)** — the
  Pegasus-ISI project that converts `pegasus-monitord` output into the JSONL
  event stream this deployment ingests. Its event schema is the data contract the
  Vector and Elasticsearch configs here track.

## License

This project is released under the Apache License 2.0 — see
[`LICENSE`](LICENSE). The acknowledgments above reference independently licensed
upstream projects; consult each for its own license terms.

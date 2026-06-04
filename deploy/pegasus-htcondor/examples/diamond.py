#!/usr/bin/env python3
"""Generate a tiny Pegasus "diamond" workflow + catalogs for the pool smoke test.

Run on the submit node, inside the runs dir. This only *writes* the abstract
workflow and the three catalogs; provision_pegasus_slice.py --run-example then
drives ``pegasus-plan --submit`` against the HTCondor ``condorpool`` site and
points workflow-monitor at the run so events flow to Elasticsearch via Vector.

Data config is **condorio**: HTCondor ships inputs/outputs to and from the
execute nodes, so no shared filesystem is assumed across the pool. ``pegasus-keg``
ships with Pegasus, which the bootstrap installs on every node, so the
``installed`` transformations resolve on the workers.
"""

from pathlib import Path

from Pegasus.api import (
    Arch,
    Directory,
    File,
    FileServer,
    Job,
    OS,
    Operation,
    Properties,
    ReplicaCatalog,
    Site,
    SiteCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)

CWD = Path.cwd()
KEG = "/usr/bin/pegasus-keg"  # provided by the Pegasus package on every node

# --- Properties: HTCondor file transfers (no shared fs across the pool) -----
props = Properties()
props["pegasus.data.configuration"] = "condorio"
props.write()

# --- Input file + Replica Catalog -------------------------------------------
fa = File("f.a")
(CWD / "f.a").write_text("pegasus diamond smoke-test input\n")
rc = ReplicaCatalog()
rc.add_replica("local", fa, str(CWD / "f.a"))
rc.write()

# --- Transformation Catalog: pegasus-keg, installed on every node -----------
tc = TransformationCatalog()
for name in ("preprocess", "findrange", "analyze"):
    tc.add_transformations(
        Transformation(name, site="condorpool", pfn=KEG, is_stageable=False)
    )
tc.write()

# --- Site Catalog: local (final output) + condorpool (vanilla universe) -----
sc = SiteCatalog()
local = Site("local", arch=Arch.X86_64, os_type=OS.LINUX)
local.add_directories(
    Directory(Directory.SHARED_SCRATCH, str(CWD / "scratch")).add_file_servers(
        FileServer("file://" + str(CWD / "scratch"), Operation.ALL)
    ),
    Directory(Directory.LOCAL_STORAGE, str(CWD / "output")).add_file_servers(
        FileServer("file://" + str(CWD / "output"), Operation.ALL)
    ),
)
condorpool = Site("condorpool", arch=Arch.X86_64, os_type=OS.LINUX)
condorpool.add_condor_profile(universe="vanilla")
condorpool.add_pegasus_profile(style="condor")
sc.add_sites(local, condorpool)
sc.write()

# --- Abstract workflow (the diamond DAG) ------------------------------------
fb1, fb2, fc1, fc2, fd = (File(f"f.{x}") for x in ("b1", "b2", "c1", "c2", "d"))
wf = Workflow("diamond")
wf.add_jobs(
    Job("preprocess")
    .add_args("-a", "preprocess", "-T", "5", "-i", fa, "-o", fb1, fb2)
    .add_inputs(fa)
    .add_outputs(fb1, fb2),
    Job("findrange")
    .add_args("-a", "findrange", "-T", "5", "-i", fb1, "-o", fc1)
    .add_inputs(fb1)
    .add_outputs(fc1),
    Job("findrange")
    .add_args("-a", "findrange", "-T", "5", "-i", fb2, "-o", fc2)
    .add_inputs(fb2)
    .add_outputs(fc2),
    Job("analyze")
    .add_args("-a", "analyze", "-T", "5", "-i", fc1, fc2, "-o", fd)
    .add_inputs(fc1, fc2)
    .add_outputs(fd, stage_out=True, register_replica=False),
)
wf.write("diamond-workflow.yml")

print(
    "wrote diamond-workflow.yml + replicas.yml + transformations.yml + "
    "sites.yml + pegasus.properties"
)

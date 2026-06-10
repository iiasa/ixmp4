import ixmp4

mp = ixmp4.Platform("pgtest")
run = mp.runs.list().pop()
cp = run.checkpoints.list().pop()
print(cp.iamc.difference())

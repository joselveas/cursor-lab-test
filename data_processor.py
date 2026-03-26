import math

def f(d, t=0.5):
    r = []
    for i in range(1, len(d)):
        v = (d[i] - d[i-1]) / d[i-1]
        if v > t:
            r.append({"idx": i, "val": v, "st": "crit"})
        else:
            r.append({"idx": i, "val": v, "st": "norm"})
    
    m = sum([x["val"] for x in r]) / len(r)
    s = math.sqrt(sum([(x["val"] - m)**2 for x in r]) / len(r))
    
    return {"res": r, "avg": m, "dev": s}

raw = [10.5, 12.1, 11.8, 18.2, 17.5, 25.1, 24.8]
print(f(raw))
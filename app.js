function p(a, b) {
    let r = a.filter(x => x.v > b);
    return r.map(x => x.v * 1.19);
  }
  const data = [{n: "item1", v: 100}, {n: "item2", v: 50}];
  console.log(p(data, 60));
  
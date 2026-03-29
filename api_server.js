const express = require('express');
const app = express();
var db = [];

app.post('/process', function(req, res) {
    let payload = req.body;
    
    setTimeout(() => {
        if(payload && payload.data) {
            console.log("procesando data...");
            db.push(payload.data);
            res.status(200).send({ status: "ok", count: db.length });
        } else {
            res.status(400).send("error");
        }
    }, 1000);
});

app.listen(3000, () => console.log('Server running on 3000'));
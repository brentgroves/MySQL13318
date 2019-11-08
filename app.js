const mysql = require('mysql');
const mqtt = require('mqtt');
const ObjectToMySql = require('./ObjectToMySql');

function insert(plexRec) {
  var p = ObjectToMySql.ObjectToMySql(plexRec);
  var con = mysql.createConnection({
    // host: 'ec2-3-14-133-181.us-east-2.compute.amazonaws.com',
    host: 'localhost',
    user: 'brent',
    password: 'JesusLives1!',
    insecureAuth: true,
    database: 'mach2',
  });

  con.connect(function(err) {
    let sql =
      `insert into DS13318 (` +
      `TransDate,PCN,ProdServer,Cycle_Counter_Shift_SL,Part_No,Name,Multiple, ` +
      `Container_Note,Cavity_Status_Key,Container_Status, ` +
      `Defect_Type,Serial_No,Setup_Container_Key,Count, ` +
      `Part_Count,Part_Key,Part_Operation_Key,Standard_Container_Type,Container_Type_Key, ` +
      `Parent_Part,Parent,Cavity_No,Master_Unit_Key,Workcenter_Printer_Key, ` +
      `Master_Unit_No,Physical_Printer_Name,Container_Count,Container_Quantity, ` +
      `Default_Printer,Default_Printer_Key,Class_Key,Quantity,Companion, ` +
      `Container_Type,Container_Type_Description,Sort_Order ` +
      `) values ( ` +
      `"${p.TransDate}","${p.PCN}",${p.ProdServer},${p.Cycle_Counter_Shift_SL},"${p.Part_No}","${p.Name}", ${p.Multiple},` +
      `"${p.Container_Note}",${p.Cavity_Status_Key},"${p.Container_Status}", ` +
      `"${p.Defect_Type}","${p.Serial_No}",${p.Setup_Container_Key},${p.Count}, ` +
      `"${p.Part_Count}",${p.Part_Key},${p.Part_Operation_Key},"${p.Standard_Container_Type}",${p.Container_Type_Key}, ` +
      `"${p.Parent_Part}","${p.Parent}","${p.Cavity_No}",${p.Master_Unit_Key},${p.Workcenter_Printer_Key}, ` +
      `"${p.Master_Unit_No}","${p.Physical_Printer_Name}",${p.Container_Count},${p.Container_Quantity}, ` +
      `"${p.Default_Printer}",${p.Default_Printer_Key},${p.Class_Key},${p.Quantity},${p.Companion}, ` +
      `"${p.Container_Type}","${p.Container_Type_Description}",${p.Sort_Order} ` +
      `)`;
    console.log(sql);
    if (err) throw err;
    con.query(sql, function(err, result) {
      if (err) throw err;
      console.log('Result: ' + result);
    });
  });
}

function main() {
  let mqttClient = mqtt.connect(
    // 'mqtt://ec2-3-15-151-115.us-east-2.compute.amazonaws.com',
    // 'mqtt://test.mosquitto.org'
    'mqtt://localhost',
  );

  mqttClient.on('connect', function() {
    mqttClient.subscribe('Plex13318', function(err) {
      if (!err) {
        console.log('subscribed to: Plex13318');
      }
    });
  });
  // message is a buffer
  mqttClient.on('message', function(topic, message) {
    const params = JSON.parse(message.toString()); // payload is a buffer
    // console.log(params);
    insert(params);
  });
}
main();


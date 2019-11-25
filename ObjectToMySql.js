module.exports.ObjectToMySql = function(params) {
  var retParams = params;

  var propTypes = {
    Control_Panel_Setup_Containers_Get_Key: 'int',
    TransDate: 'datetime',
    ProdServer: 'bool',
    Part_No: 'varchar',
    Name: 'varchar',
    Multiple: 'bool',
    Container_Note: 'varchar',
    Cavity_Status_Key: 'int',
    Container_Status: 'varchar',
    Defect_Type: 'varchar',
    Serial_No: 'varchar',
    Setup_Container_Key: 'int',
    Count: 'mediumint',
    Part_Count: 'mediumint',
    Part_Key: 'int',
    Part_Operation_Key: 'int',
    Standard_Container_Type: 'varchar',
    Container_Type_Key: 'int',
    Parent_Part: 'varchar',
    Parent: 'varchar',
    Cavity_No: 'varchar',
    Master_Unit_Key: 'int',
    Workcenter_Printer_Key: 'int',
    Master_Unit_No: 'varchar',
    Physical_Printer_Name: 'varchar',
    Container_Count: 'mediumint',
    Container_Quantity: 'mediumint',
    Default_Printer: 'varchar',
    Default_Printer_Key: 'int',
    Class_Key: 'int',
    Quantity: 'int',
    Companion: 'bool',
    Container_Type: 'varchar',
    Container_Type_Description: 'varchar',
    Sort_Order: 'mediumint',
  };

  for (var prop in retParams) {
    if ('mediumint' == propTypes[prop] && '' == retParams[prop]) retParams[prop] = null;
    if ('int' == propTypes[prop] && '' == retParams[prop]) retParams[prop] = null;
    // console.log(prop + ' ' + retParams[prop]);
  }
  return retParams;
};

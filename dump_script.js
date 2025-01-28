const mysql = require('mysql');
const fs = require('fs');
const { loadEnvFile } = require('process');

const connection = mysql.createConnection({
  host: process.env.host,
  user: process.env.user,
  password: process.env.password,
  database: process.env.database
});

const parent = '';

const batches = fs.readFileSync('uids_output.txt', 'utf-8').trim().split('\n');
//console.log(loginid);
const queries = [
  //`select * from ck_user_parent where find_in_set(userloginid, '${loginid}') <> 0 order by userloginid`,
  //`select * from ck_hierarchy_metadata where find_in_set(parent, '${loginid}') <> 0 order by parent`,
  `select * from ck_user where loginid in (?)` ,// check location
  //`select * from ck_userdesignation where find_in_set(login_id, '${loginid}') <> 0 order by login_id`,
  `select * from ck_outlet_details where outletcode in (?)`
 // `select * from ck_user_roles where find_in_set(user_id, '${loginid}') <> 0 order by user_id`
];

function createLocationSet(batchIndex){
  const rawData = fs.readFileSync('ck_user_batch_' + batchIndex + '.json', 'utf8');
  const dataArray = JSON.parse(rawData);
  return dataArray.map(item => item.location_hierarchy).join(',');
}

function createRolesSet(batchIndex){
  const rawData = fs.readFileSync('ck_user_batch_' + batchIndex + '.json', 'utf8');
  const dataArray = JSON.parse(rawData);
  return dataArray.map(item => item.id).join(',');
}

connection.connect(async (err) => {
  if (err) {
    console.error('Error connecting to the database:', err);
    return;
  }
  console.log('Connected to the database.');

  for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
    const currentBatch = batches[batchIndex];
    const loginIds = currentBatch.split(',');
    console.log(`\nProcessing batch ${batchIndex + 1}/${batches.length}...`);

    for (let queryIndex = 0; queryIndex < queries.length; queryIndex++) {
      const query = queries[queryIndex];
      const tableName = query.match(/from\s+(\w+)/i)[1];

      console.log(`Executing query for batch ${batchIndex + 1}, query ${queryIndex + 1}: ${query}`);
      try {
        const result = await new Promise((resolve, reject) => {
          connection.query(query, [loginIds], (error, results) => {
            if (error) {
              reject(error);
            } else {
              resolve(results);
            }
          });
        });

        console.log(`Query executed successfully for batch ${batchIndex + 1}, query ${queryIndex + 1}.`);
        const fileName = `${tableName}_batch_${batchIndex + 1}.json`;
        fs.writeFileSync(fileName, JSON.stringify(result, null, 2));
        console.log(`Results saved to ${fileName}`);
      } catch (error) {
        console.error(`Error executing query for batch ${batchIndex + 1}, query ${queryIndex + 1}:`, error);
        return;
      }
    }
    
    //  const locationSet = createLocationSet(batchIndex+1);
    // console.log(locationSet);
    //  const query1 = `select * from ck_location where find_in_set(location_hierarchy, '${locationSet}') <> 0`
     // await execQuery(query1,batchIndex+1);
      // const roleSet = createRolesSet(batchIndex+1);
      // const query2 = `select * from ck_user_roles where find_in_set(user_id, '${roleSet}') <> 0`
      // await execQuery(query2,batchIndex+1);
    
  }

  // const locationSet = createLocationSet();
  // console.log(locationSet);
  // const query1 = `select * from ck_location where find_in_set(location_hierarchy, '${locationSet}') <> 0`

async function execQuery(query1,batchIndex) {
  try {
    const tableName = query1.match(/from\s+(\w+)/i)[1];
    const result = await new Promise((resolve, reject) => {
      connection.query(query1, (error, results) => {
        if (error) {
          reject(error);
        } else {
          resolve(results);
        }
      });
    });
    console.log(`Query executed successfully.`);
   // fs.appendFileSync(match + ".json", '' + query + '\n');
    //console.log(`Query ${index + 1} written to ${config.fileName}.`);
    fs.writeFileSync(tableName +"_batch_" +batchIndex+ ".json", JSON.stringify(result,null,2) + '\n');
    //console.log(`Result of query ${index + 1} written to ${config.fileName}.`);
  } catch (error) {
    console.error('Error executing query or writing to file:', query1, error);
    return;
  }
}


  // const roleSet = createRolesSet();
  // console.log(roleSet);
  // const query2 = `select * from ck_user_roles where find_in_set(user_id, '${roleSet}') <> 0`

  // try {
  //   const result = await new Promise((resolve, reject) => {
  //     connection.query(query2, (error, results) => {
  //       if (error) {
  //         reject(error);
  //       } else {
  //         resolve(results);
  //       }
  //     });
  //   });
  //   console.log(`Query executed successfully.`);
  //  // fs.appendFileSync(match + ".json", '' + query + '\n');
  //   //console.log(`Query ${index + 1} written to ${config.fileName}.`);
  //   fs.writeFileSync("ck_user_roles" + ".json", JSON.stringify(result,null,2) + '\n');
  //   //console.log(`Result of query ${index + 1} written to ${config.fileName}.`);
  // } catch (error) {
  //   console.error('Error executing query or writing to file:', query2, error);
  //   return;
  // }

 //console.log(`All results saved to ${config.fileName}`);
  connection.end(err => {
    if (err) {
      console.error('Error closing the connection:', err);
    } else {
      console.log('Database connection closed.');
    }
  });
});


// -- select * from ck_user_parent where userloginid in (@loginid);
// -- select * from ck_hierarchy_metadata where parent in (@loginid);
// -- select * from ck_user where loginid in (@loginid); -- check location
// -- select * from ck_userdesignation where login_id in (@loginid);
// -- select * from ck_outlet_details where outletcode in (@loginid);
// -- select * from ck_user_roles where user_id in (@loginid);

// -- select * from ck_lcation where location like @location
// -- select * from ck_outlet_details_hierarchymetadata where outlet_id = (select id from ck_outlet_details where outletcode = @loginid);
// -- select * from ck_integration_history where entity_id in (select id from ck_outlet_details where outletcode = @loginid);

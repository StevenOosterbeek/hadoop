var https = require('https'),
    mongoose = require('mongoose'),
    db = mongoose.connection,
    Employee = require('./employee');

// Make a connection to the MongoDB
mongoose.connect('mongodb://localhost/cityOfChicago');
db.on('error', console.error.bind(console, 'Mongoose connection error!'));
db.once('open', function () { console.log('\nConnection with cityOfChicago database established, now getting API data \n'); });

// Load the data out of the API
https.get('https://data.cityofchicago.org/resource/xzkq-xp2w.json', function (res) {

    var data, rawData = '';

    res.on('data', function (d) {
        rawData += d;
    });

    res.on('end', function () {
        data = JSON.parse(rawData);
        data.forEach(function (record) {

            var newEmployee = {
                name: record.name,
                department: record.department,
                job_title: record.job_titles,
                annual_salary: record.employee_annual_salary
            };

            Employee.create(newEmployee, function (err, createdRecord) {
                if (err) console.log('Error while creating new employee record:', err);
                else console.log('Saved record from: ', newEmployee.name);
            });

        });
    });

}).on('error', function (e) {
  console.log('Something went wrong:', e.message);
});
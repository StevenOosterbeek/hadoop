var mongoose = require('mongoose'),
    Schema = mongoose.Schema;

var employeeSchema = new Schema({
    name: String,
    department: String,
    job_title: String,
    annual_salary: Number
});

module.exports = mongoose.model('Employee', employeeSchema);
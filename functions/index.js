const functions = require('firebase-functions');

// // Create and Deploy Your First Cloud Functions
// // https://firebase.google.com/docs/functions/write-firebase-functions
//
// exports.helloWorld = functions.https.onRequest((request, response) => {
//   functions.logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });

// The Firebase Admin SDK to access Cloud Firestore.
const admin = require('firebase-admin');
admin.initializeApp();

exports.newDataAdded_nb_days = functions.database.ref('/ngt-inv-corr/data_quality_checks/{instrument}/Nb_data_points')
    .onWrite((change, context) => {

        if (change.before.ref.parent.child('Total_nb_data_points').exists()){
            const before_agg = change.before.ref.parent.child('Total_nb_data_points');
            const after_nd_dps = change.after.ref;

            const new_agg = before_agg + after_nd_dps;
            return change.after.ref.parent.child('Total_nb_data_points').set(new_agg);
        }  else {
            const before_val = change.before.ref;
            const after_val = change.after.ref;

            const new_agg = before_val + after_val;
            return change.after.ref.parent.child('Total_nb_data_points').set(new_agg);
        }
    });

exports.newDataAdded_miss_days = functions.database.ref('/ngt-inv-corr/data_quality_checks/{instrument}/Nb_Val_Days_Missing')
    .onWrite((change, context) => {

        if (change.before.ref.parent.child('Total_nb_Val_Days_Missing').exists()){
            const before_agg = change.before.ref.parent.child('Nb_Val_Days_Missing');
            const after_val = change.after.ref;

            const new_agg = before_agg + after_val;
            return change.after.ref.parent.child('Total_nb_Val_Days_Missing').set(new_agg);
        }  else {
            const before_val = change.before.ref;
            const after_val = change.after.ref;

            const new_agg = before_val + after_val;
            return change.after.ref.parent.child('Total_nb_Val_Days_Missing').set(new_agg);
        }
    });

exports.newDataAdded_new_mean = functions.database.ref('/ngt-inv-corr/data_quality_checks/{instrument}/Mean_NAV')
    .onWrite((change, context) => {

        if (change.before.ref.parent.child('Overall_mean_NAV').exists()){
            const before_mean = change.before.ref.parent.child('Overall_mean_NAV');
            const before_total_days = change.before.ref.parent.child('Total_nb_data_points')
            const after_mean = change.after.ref;
            const after_nb_days = change.after.ref.parent.child('Nb_data_points')

            const new_agg = ((before_mean * before_total_days) + (after_mean * after_nb_days)) / (before_total_days + after_nb_days);

            return change.after.ref.parent.child('Overall_mean_NAV').set(new_agg);
        }  else {
            const before_mean = change.before.ref;
            const before_nb_days = change.before.ref.parent.child('Nb_data_points')
            const after_mean = change.after.ref;
            const after_nb_days = change.after.ref.parent.child('Nb_data_points')

            const new_agg = (before_mean * before_nb_days + after_mean * after_nb_days) / (before_nb_days + after_nb_days)
            return change.after.ref.parent.child('Overall_mean_NAV').set(new_agg);
        }
    });
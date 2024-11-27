import psycopg
import numpy as np

class DatabaseConnection(object):
    def __init__(self, machine_id = 1):
        self.machine_id = machine_id

    def get_measurement(self, traj_id = 140):
        ## retrieve measurement log for the needed quantities
        measurement_quantities = ["position", "angular position"]
        conn = psycopg.connect("host=127.0.0.1 dbname=gantrycrane user=postgres password=postgres")
        cur = conn.cursor()
        query = "select ts, value, quantity from measurement " \
                        + "where machine_id = " + str(self.machine_id) \
                        + " and run_id = " + str(traj_id) \
                        + " and quantity in (" + str(measurement_quantities)[1:-1] + ")" \
                        + " order by quantity, ts;"
        cur.execute(query)
        ret = cur.fetchall()
        ts = np.array([row[0] for row in ret])
        value = np.array([row[1] for row in ret])
        quantity = np.array([row[2] for row in ret])
        measurement = {}
        for qty in measurement_quantities:
            idx = quantity == qty
            time_list = np.array([td.total_seconds() for td in (ts[idx] - ts[idx][0])]).tolist()
            value_list = value[idx].tolist()
            time_value_pair_list = list(zip(time_list, value_list))
            measurement[qty] = time_value_pair_list
        conn.close()
        return measurement

    def get_trajectory(self, traj_id = 140):
        ## retrieve measurement log for the needed quantities
        measurement_quantities = ["position", "angular position"]
        conn = psycopg.connect("host=127.0.0.1 dbname=gantrycrane user=postgres password=postgres")
        cur = conn.cursor()
        query = "select ts, value, quantity from trajectory " \
                        + "where machine_id = " + str(self.machine_id) \
                        + " and run_id = " + str(traj_id) \
                        + " and quantity in (" + str(measurement_quantities)[1:-1] + ")" \
                        + " order by quantity, ts;"
        cur.execute(query)
        ret = cur.fetchall()
        ts = np.array([row[0] for row in ret])
        value = np.array([row[1] for row in ret])
        quantity = np.array([row[2] for row in ret])
        trajectory = {}
        for qty in measurement_quantities:
            idx = quantity == qty
            time_list = np.array([td.total_seconds() for td in (ts[idx] - ts[idx][0])]).tolist()
            value_list = value[idx].tolist()
            time_value_pair_list = list(zip(time_list, value_list))
            trajectory[qty] = time_value_pair_list
        conn.close()
        return trajectory
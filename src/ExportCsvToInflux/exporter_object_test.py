from unittest import TestCase
from unittest.mock import patch
from unittest.mock import Mock

from ExportCsvToInflux.exporter_object import ExporterObject
from ExportCsvToInflux.influx_object import InfluxObject


class Test_exporter_object(TestCase):

    def test_water_quality_sample_data(self):
        """ test on water quality sample data """

        mock_client = Mock()
        mock_client.write_points = Mock()
        mock_client.switch_user = Mock()

        with patch.object(
            InfluxObject, 'create_influx_db_if_not_exists',
            return_value=mock_client
        ):
            COLUMNS = (
                "NOX-S,NOX-B,NO3_S,NO3_B,NO2-S,NO2-B,NH4-S,NH4-B,TN-S,TN-B,"
                "DIN-S,DIN-B,TON-S,TON-B,TP-S,TP-B,SRP-S,SRP-B,APA-S,APA-B,"
                "CHLA-S,CHLA-B,TOC-S,TOC-B,SiO2-S,SiO2-B,TURB-S,TURB-B,SAL-S,"
                "SAL-B,TEMP-S,TEMP-B,DO-S,DO-B,Kd,pH,TN:TP,N:P,DIN:TP,"
                "Si:DIN,%SAT-S,%SAT_B,%Io,DSIGT"
            )

            exporter = ExporterObject()
            exporter.export_csv_to_influx(
                csv_file="test_data/water_quality_data.csv",
                db_name="fwc_coral_disease",
                db_measurement="Walton_Smith",
                field_columns=COLUMNS,
                force_int_columns=COLUMNS,
                tag_columns=(
                    "SURV,BASIN,SEGMENT,ZONE,STATION,SITE,LATDEC,LONDEC,DEPTH"
                ),
                # --force_insert_even_csv_no_update True
                # --server 192.168.1.247:8086
                time_column="timestamp",
                force_insert_even_csv_no_update=True
            )

            # MockInfluxObject.assert_called_with(
            #     db_server_name='localhost:8086',
            #     db_user='admin', db_password='admin'
            # )
            mock_client.switch_user.assert_called_with('admin', 'admin')
            mock_client.write_points.assert_called_with(
                [{'measurement': 'Walton_Smith', 'time': 1543502520000000000, 'fields': {'NOX-S': 0, 'NOX-B': 0, 'NO3_S': 0, 'NO3_B': 0, 'NO2-S': 0, 'NO2-B': 0, 'NH4-S': 0, 'NH4-B': 0, 'TN-S': 0, 'TN-B': 0, 'DIN-S': 0, 'DIN-B': 0, 'TON-S': 0, 'TON-B': 0, 'TP-S': 0, 'TP-B': 0, 'SRP-S': 0, 'SRP-B': 0, 'APA-S': -999, 'APA-B': -999, 'CHLA-S': 0, 'CHLA-B': -999, 'TOC-S': 1, 'TOC-B': 1, 'SiO2-S': 0, 'SiO2-B': 0, 'TURB-S': 0, 'TURB-B': 0, 'SAL-S': 37, 'SAL-B': -999, 'TEMP-S': -999, 'TEMP-B': -999, 'DO-S': -999, 'DO-B': -999, 'Kd': -999, 'pH': -999, 'TN:TP': 36, 'N:P': 28, 'DIN:TP': 1, 'Si:DIN': 4, '%SAT-S': -999, '%SAT_B': -999, '%Io': -999, 'DSIGT': -999}, 'tags': {'SURV': 94, 'BASIN': 'FK', 'SEGMENT': 'OFF', 'ZONE': 'REEF', 'STATION': 259, 'SITE': 'Big Pine Shoal', 'LATDEC': 24.57037, 'LONDEC': -81.32166999999998, 'DEPTH': 7.617}}, {'measurement': 'Walton_Smith', 'time': 1506571200000000000, 'fields': {'NOX-S': 0, 'NOX-B': 0, 'NO3_S': 0, 'NO3_B': 0, 'NO2-S': 0, 'NO2-B': 0, 'NH4-S': 0, 'NH4-B': 0, 'TN-S': 0, 'TN-B': 0, 'DIN-S': 0, 'DIN-B': 0, 'TON-S': 0, 'TON-B': 0, 'TP-S': 0, 'TP-B': 0, 'SRP-S': 0, 'SRP-B': 0, 'APA-S': 1, 'APA-B': 1, 'CHLA-S': 0, 'CHLA-B': 1, 'TOC-S': 1, 'TOC-B': 1, 'SiO2-S': 0, 'SiO2-B': 0, 'TURB-S': 0, 'TURB-B': 0, 'SAL-S': 35, 'SAL-B': 35, 'TEMP-S': 30, 'TEMP-B': 30, 'DO-S': 6, 'DO-B': 6, 'Kd': 0, 'pH': 1, 'TN:TP': 48, 'N:P': 1, 'DIN:TP': 1, 'Si:DIN': 1, '%SAT-S': 94, '%SAT_B': 94, '%Io': 18, 'DSIGT': 0}, 'tags': {'SURV': 89, 'BASIN': 'FK', 'SEGMENT': 'MAR', 'ZONE': 'MARQ', 'STATION': 333, 'SITE': 'Half Moon Shoal', 'LATDEC': 24.57717, 'LONDEC': -82.49133, 'DEPTH': 16.36}}, {'measurement': 'Walton_Smith', 'time': 925785480000000000, 'fields': {'NOX-S': 0, 'NOX-B': 0, 'NO3_S': 0, 'NO3_B': 0, 'NO2-S': 0, 'NO2-B': 0, 'NH4-S': 0, 'NH4-B': 0, 'TN-S': 0, 'TN-B': 0, 'DIN-S': 0, 'DIN-B': 0, 'TON-S': 0, 'TON-B': 0, 'TP-S': 0, 'TP-B': 0, 'SRP-S': 0, 'SRP-B': 0, 'APA-S': 0, 'APA-B': 0, 'CHLA-S': 0, 'CHLA-B': -999, 'TOC-S': 3, 'TOC-B': 3, 'SiO2-S': 0, 'SiO2-B': 0, 'TURB-S': 1, 'TURB-B': 1, 'SAL-S': 36, 'SAL-B': 36, 'TEMP-S': 25, 'TEMP-B': 25, 'DO-S': 6, 'DO-B': 6, 'Kd': 0, 'pH': -999, 'TN:TP': 30, 'N:P': 8, 'DIN:TP': 1, 'Si:DIN': 8, '%SAT-S': 91, '%SAT_B': 90, '%Io': 3, 'DSIGT': 0}, 'tags': {'SURV': 16, 'BASIN': 'FK', 'SEGMENT': 'MAR', 'ZONE': 'MARQ', 'STATION': 324, 'SITE': 'Ellis Rock', 'LATDEC': 24.65333, 'LONDEC': -82.15833, 'DEPTH': 8.5}}]
            )

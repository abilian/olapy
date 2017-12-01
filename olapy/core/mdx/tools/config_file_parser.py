"""
Parse cube configuration file and create cube parser object which can be passed to the MdxEngine
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
from collections import OrderedDict

from lxml import etree

from .models import Cube, Dimension, Facts


class ConfigParser:
    """Parse olapy config (web and excel) files.

    Config file used if you want to show only some measures, dimensions,
    columns... in excel

    Config file should be under '~/olapy-data/cubes/cubes-config.xml'

    Assuming we have two tables as follows under 'labster' folder

    table 1: stats_line (which is the facts table)

    +----------------+---------+--------------------+----------------------+
    | departement_id | amount  |    monthly_salary  |  total monthly cost  |
    +----------------+---------+--------------------+----------------------+
    |  111           |  1000   |      2000          |    3000              |
    +----------------+---------+--------------------+----------------------+
    | bla  bla bla   |         |                    |                      |
    +----------------+---------+--------------------+----------------------+

    table 2: orgunit (which is a dimension)

    +------+---------------+-----------+------------------+------------------+
    | id   | type          |  name     |  acronym         | other colums.....|
    +------+---------------+-----------+------------------+------------------+
    |  111 | humanitarian  |  humania  | for better life  |                  |
    +------+---------------+-----------+------------------+------------------+
    | bla  | bla   bla     |           |                  |                  |
    +------+---------------+-----------+------------------+------------------+


    Excel Config file Structure example::

        <?xml version="1.0" encoding="UTF-8"?>

            <cubes>

                <!-- if you want to set an authentication mechanism for excel to access cube,
                    user must set a token with login url like 'http://127.0.0.1/admin  -->
                <!-- default password = admin -->

                <!-- enable/disable xmla authentication -->
                <xmla_authentication>False</xmla_authentication>

                <cube>

                    <!-- cube name => csv folder name or database name -->
                    <name>labster</name>

                    <!-- source : csv | postgres | mysql | oracle | mssql -->
                    <source>csv</source>


                    <!-- star building customized star schema -->

                    <facts>

                        <!-- facts table name -->
                        <table_name>stats_line</table_name>

                        <keys>
                            <!-- <column_name ref="[target_table_name].[target_column_name]">[Facts_column_name]</column_name> -->
                            <column_name ref="orgunit.id">departement_id</column_name>

                        </keys>

                        <!-- specify measures explicitly -->
                        <measures>

                            <!-- by default, all number type columns in facts table, or you can specify them here -->
                            <name>montant</name>
                            <name>salaire_brut_mensuel</name>
                            <name>cout_total_mensuel</name>
                        </measures>

                    </facts>
                    <!-- end building customized star schema -->


                    <!-- star building customized dimensions display in excel from the star schema -->
                    <dimensions>


                        <dimension>

                            <!-- if you want to keep the same name for excel display, just use the same name in name and displayName -->
                            <name>orgunit</name>
                            <displayName>Organisation</displayName>

                            <columns>

                                <!-- IMPORTANT !!!!  COLUMNS ORDER MATTER -->
                                <name>type</name>
                                <name>nom</name>
                                <name>sigle</name>
                            </columns>

                        </dimension>

                    </dimensions>

                    <!-- end building customized dimensions display in excel from the star schema -->


                </cube>

            </cubes>


    WEB Config file Structure (work with `olapy-web <https://github.com/abilian/olapy-web>`_)::

        <cubes>
           <cube>
              <!-- cube name => db name -->
              <name>mpr</name>

              <!-- source : postgres | csv -->
              <source>postgres</source>

              <!-- star building customized star schema -->
              <facts>
                 <!-- facts table name -->
                 <table_name>projet</table_name>

                 <keys>
                    <!-- ref = table_name.column  -->
                    <column_name ref="vocabulary_crm_status.id">status_id</column_name>
                    <column_name ref="vocabulary_crm_pole_leader.id">pole_leader_id</column_name>
                    <column_name ref="contact.id">contact_id</column_name>
                    <column_name ref="compte.id">compte_porteur_id</column_name>
                    <column_name ref="vocabulary_crm_aap_type.id">aap_name_id</column_name>
                 </keys>

                 <!-- specify measures explicitly -->
                 <measures>
                    <!-- by default, all number type columns in facts table, or you can specify them here -->
                    <name>budget_total</name>
                    <name>subvention_totale</name>
                    <name>duree_projet</name>
                 </measures>

                 <!-- additional columns to keep other than measures and ids -->
                 <columns>etat,aap,axes_de_developpement</columns>
              </facts>

              <!-- end building customized star schema -->
              <tables>
                 <!-- Table name -->
                 <table name="vocabulary_crm_status">

                    <!-- Columns to keep (INCLUDING id)-->
                    <!-- They must be seperated with comma ',' -->
                    <columns>id,label</columns>

                    <!-- Change insignificant table columns names -->
                    <!-- {IMPORTANT} Renaming COMMUN columns between dimensions and other columns if you want, other than ids column -->
                    <new_name old_column_name="label">Status</new_name>
                 </table>

                 <table name="contact">
                    <columns>id,nom,prenom,fonction</columns>
                    <new_name old_column_name="fonction">Contact Fonction</new_name>
                 </table>
              </tables>

              <!-- Dashboards -->
              <Dashboards>
                 <Dashboard>
                    <Global_table>
                       <!-- IMPORTANT !! columns and rows names must be specified as above with their new names -->
                       <!-- EXAMPLE <new_name old_column_name="label">Pole leader</new_name>, you put Pole leader -->
                       <!-- marches,axes_de_developpement,statut_pour_book are columns from facts table  -->
                       <columns>marches,axes_de_developpement</columns>
                       <rows>statut_pour_book</rows>
                    </Global_table>

                    <!-- Contact Fonction,Type Organisation columns name from different tables (with ther new names) -->
                    <PieCharts>Contact Fonction,Type Organisation</PieCharts>

                    <!-- TODO BarCharts with Stacked Bar Chart -->
                    <BarCharts>Avis</BarCharts>

                    <!-- Preferably with time/date (or sequenced) tables-->
                    <LineCharts>
                       <table>
                          <!-- date_debut_envisagee a column from facts table  -->
                          <name>date_debut_envisagee</name>
                          <!-- if not specified, then all columns attributs -->
                          <!--<columns>1945,2000,2006,2015</columns> -->
                       </table>
                    </LineCharts>
                 </Dashboard>
              </Dashboards>
            <!-- END Dashboards -->
           </cube>
        </cubes>
    """

    def __init__(
            self,
            cube_path=None,
            file_name='cubes-config.xml',
            web_config_file_name='web_cube_config.xml', ):
        """

        :param cube_path: path to cube (csv folders) where config file is located by default
        :param file_name: config file name (DEFAULT = cubes-config.xml)
        :param web_config_file_name: web config file name (DEFAULT = web_cube_config.xml)
        """

        if cube_path:
            self.cube_path = cube_path
        else:
            self.cube_path = self._get_cube_path()

        self.file_name = file_name
        self.web_config_file_name = web_config_file_name

    def _get_cube_path(self):
        if 'OLAPY_PATH' in os.environ:
            home_directory = os.environ['OLAPY_PATH']
        else:
            from os.path import expanduser
            home_directory = expanduser("~")

        return os.path.join(home_directory, 'olapy-data', 'cubes', )

    # todo one function
    def get_config_file_path(self):
        return os.path.join(self.cube_path, self.file_name)
    #
    # def get_web_confile_file_path(self):
    #     return os.path.join(self.cube_path, self.web_config_file_name)

    def config_file_exist(self):
        """
        Check whether the config file exists or not.

        :param client_type: excel config file or web
        :return: True | False
        """

        # if client_type == 'web':
        #     return os.path.isfile(self.get_web_confile_file_path())
        return os.path.isfile(self.get_config_file_path())

    def xmla_authentication(self):
        """Check if excel need authentication to access cubes or not.

        (xmla_authentication tag in the config file).

        :return: True | False
        """

        # xmla authentication only in excel
        if self.config_file_exist():
            with open(self.get_config_file_path()) as config_file:
                parser = etree.XMLParser()
                tree = etree.parse(config_file, parser)

                try:
                    return tree.xpath('/cubes/xmla_authentication')[0].text == 'True'
                except BaseException:
                    return False
        else:
            return False

    def get_cubes_names(self):
        """Get all cubes names in the config file.

        :return: dict with dict name as key and cube source as value (csv | postgres | mysql | oracle | mssql)
        """
        # if client_type == 'excel':
        file_path = self.get_config_file_path()
        # elif client_type == 'web':
        #     file_path = self.get_web_confile_file_path()
        # else:
        #     raise ValueError("Unknown client_type: {}".format(client_type))
        with open(file_path) as config_file:
            parser = etree.XMLParser()
            tree = etree.parse(config_file, parser)

            try:
                return {
                    cube.find('name').text: cube.find('source').text
                    for cube in tree.xpath('/cubes/cube')
                }
            except BaseException:  # pragma: no cover
                raise ValueError('missed name or source tags')

    def _construct_cubes_excel(self):
        """
        Construct parser cube obj (which can ben passed to MdxEngine) for excel
        :return: Cube obj
        """
        try:
            with open(self.get_config_file_path()) as config_file:
                parser = etree.XMLParser()
                tree = etree.parse(config_file, parser)

                facts = [
                    Facts(
                        table_name=xml_facts.find('table_name').text,
                        keys={
                            key.text: key.attrib['ref']
                            for key in xml_facts.findall('keys/column_name')
                        },
                        measures=[
                            mes.text
                            for mes in xml_facts.findall('measures/name')
                        ], ) for xml_facts in tree.xpath('/cubes/cube/facts')
                ]

                dimensions = [
                    Dimension(
                        name=xml_dimension.find('name').text,
                        column_new_name=[key.attrib['column_new_name']
                                         for key in xml_dimension.findall('name')],
                        displayName=xml_dimension.find('displayName').text,
                        columns=OrderedDict(
                            (
                                column_name.text,
                                column_name.text if not column_name.attrib else
                                column_name.attrib['column_new_name'],)
                            for column_name in xml_dimension.findall(
                                'columns/name', )), )
                    for xml_dimension in tree.xpath(
                        '/cubes/cube/dimensions/dimension', )
                ]

            return [
                Cube(
                    name=xml_cube.find('name').text,
                    source=xml_cube.find('source').text,
                    facts=facts,
                    dimensions=dimensions, )
                for xml_cube in tree.xpath('/cubes/cube')
            ]
        except BaseException:
            raise ValueError('Bad configuration in the configuration file')

    def construct_cubes(self):
        """Construct cube based on config file.

        :param client_type: excel | web
        :return: list of Cubes instance
        """

        if self.config_file_exist():
            return self._construct_cubes_excel()
            # elif client_type == 'web':
            #     return self._construct_cubes_web()

        else:
            raise ValueError("Config file doesn't exist")

    # def _construct_cubes_web(self):
    #     """
    #     Construct parser cube obj (which can ben passed to MdxEngine) for web
    #     :return: Cube obj
    #     """
    #     # todo fix path with instance path
    #     with open(self.get_web_confile_file_path()) as config_file:
    #         parser = etree.XMLParser()
    #         tree = etree.parse(config_file, parser)
    #         facts = [
    #             Facts(
    #                 table_name=xml_facts.find('table_name').text,
    #                 keys={
    #                     key.text: key.attrib['ref']
    #                     for key in xml_facts.findall('keys/column_name')
    #                 },
    #                 measures=[
    #                     mes.text for mes in xml_facts.findall('measures/name')
    #                 ],
    #                 columns=xml_facts.find('columns').text.split(',') if xml_facts.find('columns') else '',
    #             )
    #
    #             for xml_facts in tree.xpath('/cubes/cube/facts')
    #         ]
    #
    #         tables = [
    #             Table(
    #                 name=xml_column.attrib['name'],
    #                 columns=xml_column.find('columns').text.split(','),
    #                 new_names={
    #                     new_col.attrib['old_column_name']: new_col.text
    #                     for new_col in xml_column.findall('new_name')
    #                 },
    #             ) for xml_column in tree.xpath('/cubes/cube/tables/table')
    #         ]
    #
    #     return [
    #         Cube(
    #             name=xml_cube.find('name').text,
    #             source=xml_cube.find('source').text,
    #             facts=facts,
    #             tables=tables, ) for xml_cube in tree.xpath('/cubes/cube')
    #     ]

    # def construct_web_dashboard(self):
    #     with open(self.get_web_confile_file_path()) as config_file:
    #         parser = etree.XMLParser()
    #         tree = etree.parse(config_file, parser)
    #
    #     return [
    #         Dashboard(
    #             global_table={
    #                 'columns':
    #                     dashboard.find('Global_table/columns').text.split(','),
    #                 'rows':
    #                     dashboard.find('Global_table/rows').text.split(','),
    #             },
    #             pie_charts=dashboard.find('PieCharts').text.split(','),
    #             bar_charts=dashboard.find('BarCharts').text.split(','),
    #             line_charts={
    #                 table.find('name').text:
    #                     (table.find('columns').text.split(',')
    #                      if table.find('columns') is not None else 'ALL')
    #                 for table in dashboard.findall('LineCharts/table')
    #             }, )
    #         for dashboard in tree.xpath('/cubes/cube/Dashboards/Dashboard')
    #     ]

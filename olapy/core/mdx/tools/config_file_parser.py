from __future__ import absolute_import, division, print_function

import os
from collections import OrderedDict

from lxml import etree

from .models import Cube, Dimension, Facts, Table, Dashboard


class ConfigParser:
    """
    Parse olapy config files.

    Config file used if you want to show only some measures, dimensions, columns... in excel

    Config file should be under 'home-directory/olapy-data/cubes/cubes-config.xml'

    Excel Config file Structure::


            <?xml version="1.0" encoding="UTF-8"?>

                <cubes>

                    <!-- if you want to set an authentication mechanism in excel so to access cube, 
                        user must set a token with login url like 'http://127.0.0.1/admin  -->

                    <!-- default password = admin -->

                    <xmla_authentication>False</xmla_authentication>

                    <cube>
                        <!-- cube name => db name -->

                        <name>labster</name>

                        <!-- source : postgres | csv -->

                        <source>postgres</source>


                        <!-- star building customized star schema -->
                        <facts>

                            <!-- facts table name -->

                            <table_name>stats_line</table_name>

                            <keys>

                                <!-- ref = table_name.column  -->

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

                            <!-- ADD facts table name to the dimensions section like this (this is a little bug to be solved soon) -->

                             <dimension>
                                <name>stats_line</name>
                                <displayName>stats_line</displayName>
                            </dimension>

                            <dimension>

                                <!-- if you want to keep the same name for excel display, just use the same name in name and displayName -->

                                <name>stats_line</name>
                                <displayName>Demande</displayName>

                                <columns>

                                    <!-- columns order matter -->
                                    <!-- column_new_name if you want to change column display name in excel -->
                                    <!-- if you don't want to change display name , rewrite it in column_new_name -->
                                    <name column_new_name="Type">type_demande</name>
                                    <name column_new_name="Financeur">financeur</name>
                                    <name column_new_name="Etat">wf_state</name>
                                    <name column_new_name="type_recrutement">type_recrutement</name>

                                </columns>

                            </dimension>

                            <dimension>

                                <!-- if you want to keep the same name for excel display, just use the same name in name and displayName -->
                                <name>orgunit</name>
                                <displayName>Organisation</displayName>

                                <columns>
                                    <!-- columns order matter -->
                                    <name column_new_name="type">type</name>
                                    <name column_new_name="Nom">nom</name>
                                    <name column_new_name="SIGLE">sigle</name>
                                </columns>

                            </dimension>

                        </dimensions>

                        <!-- end building customized dimensions display in excel from the star schema -->


                    </cube>

                </cubes>


    WEB Config file Structure::


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

    def __init__(self,
                 cube_path=None,
                 file_name='cubes-config.xml',
                 web_config_file_name='web_cube_config.xml'):
        """

        :param cube_path: path to cube (csv folders)
        :param file_name: config file name (DEFAULT = cubes-config.xml)
        """
        # home_directory = home_directory
        if 'OLAPY_PATH' in os.environ:
            home_directory = os.environ['OLAPY_PATH']
        else:
            from os.path import expanduser
            home_directory = expanduser("~")

        if cube_path is None:
            self.cube_path = os.path.join(home_directory, 'olapy-data',
                                          'cubes')
        else:
            self.cube_path = cube_path

        self.file_name = file_name
        self.web_config_file_name = web_config_file_name

    def config_file_exist(self, client_type):
        """
        Check whether the config file exists or not.

        :return: True | False
        """
        if client_type == 'web':
            return os.path.isfile(
                os.path.join(self.cube_path, self.web_config_file_name))
        return os.path.isfile(os.path.join(self.cube_path, self.file_name))

    def xmla_authentication(self):
        """
        Check if excel need authentication to access cubes or not. (xmla_authentication tag in the config file).

        :return: True | False
        """

        # xmla authentication only in excel
        if self.config_file_exist(client_type='excel'):
            with open(os.path.join(self.cube_path,
                                   self.file_name)) as config_file:

                parser = etree.XMLParser()
                tree = etree.parse(config_file, parser)

                try:
                    return tree.xpath('/cubes/xmla_authentication')[
                        0].text == 'True'
                except:
                    return False
        else:
            return False

    def get_cubes_names(self, client_type):
        """
        Get all cubes names in the config file.

        :return: dict of cube name as key and cube source as value (csv or postgres) (right now only postgres is supported)
        """
        if client_type == 'excel':
            file_name = self.file_name
        elif client_type == 'web':
            file_name = self.web_config_file_name

        with open(os.path.join(self.cube_path, file_name)) as config_file:

            parser = etree.XMLParser()
            tree = etree.parse(config_file, parser)

            try:
                return {
                    cube.find('name').text: cube.find('source').text
                    for cube in tree.xpath('/cubes/cube')
                }
            except:
                raise ('missed name or source tags')

    def _construct_cubes_excel(self):
        try:
            with open(os.path.join(self.cube_path,
                                   self.file_name)) as config_file:

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
                        ]) for xml_facts in tree.xpath('/cubes/cube/facts')
                ]

                # keys = {
                #            key.text: key.attrib['ref']
                #            for key in xml_facts.findall('keys/column_name')
                #        },

                dimensions = [
                    Dimension(
                        name=xml_dimension.find('name').text,
                        # column_new_name = [key.attrib['column_new_name'] for key in xml_dimension.findall('name')],
                        displayName=xml_dimension.find('displayName').text,
                        columns=OrderedDict(
                            (column_name.text, None if not column_name.attrib
                             else column_name.attrib['column_new_name'])
                            for column_name in xml_dimension.findall(
                                'columns/name')))
                    for xml_dimension in tree.xpath(
                        '/cubes/cube/dimensions/dimension')
                ]

            return [
                Cube(
                    name=xml_cube.find('name').text,
                    source=xml_cube.find('source').text,
                    facts=facts,
                    dimensions=dimensions)
                for xml_cube in tree.xpath('/cubes/cube')
            ]
        except:
            raise ('Bad configuration in the configuration file')

    def construct_cubes(self, client_type='excel'):
        """
        Construct cube (with it dimensions) and facts from  the config file.
        :param client_type: excel | web
        :return: list of Cubes instance
        """

        if self.config_file_exist(client_type):
            if client_type == 'excel':
                return self._construct_cubes_excel()
            elif client_type == 'web':
                return self._construct_cubes_web()

        else:
            raise ("Config file don't exist")

    def _construct_cubes_web(self):

        # try:
        with open(os.path.join(self.cube_path,
                               self.web_config_file_name)) as config_file:
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
                        mes.text for mes in xml_facts.findall('measures/name')
                    ],
                    columns=xml_facts.find('columns').text.split(','))
                for xml_facts in tree.xpath('/cubes/cube/facts')
            ]

            tables = [
                Table(
                    name=xml_column.attrib['name'],
                    columns=xml_column.find('columns').text.split(','),
                    new_names={
                        new_col.attrib['old_column_name']: new_col.text
                        for new_col in xml_column.findall('new_name')
                    }) for xml_column in tree.xpath('/cubes/cube/tables/table')
            ]

        return [
            Cube(
                name=xml_cube.find('name').text,
                source=xml_cube.find('source').text,
                facts=facts,
                tables=tables) for xml_cube in tree.xpath('/cubes/cube')
        ]
        # except:
        #     raise ('Bad configuration in the configuration file')

    def construct_web_dashboard(self):

        # try:
        with open(os.path.join(self.cube_path,
                               self.web_config_file_name)) as config_file:
            parser = etree.XMLParser()
            tree = etree.parse(config_file, parser)

        return [
            Dashboard(
                global_table={
                    'columns':
                    dashboard.find('Global_table/columns').text.split(','),
                    'rows': dashboard.find('Global_table/rows').text.split(',')
                },
                pie_charts=dashboard.find('PieCharts').text.split(','),
                bar_charts=dashboard.find('BarCharts').text.split(','),
                line_charts={
                    table.find('name').text:
                    (table.find('columns').text.split(',')
                     if table.find('columns') is not None else 'ALL')
                    for table in dashboard.findall('LineCharts/table')
                })
            for dashboard in tree.xpath('/cubes/cube/Dashboards/Dashboard')
        ]
        # except:
        #     raise ('Bad configuration in the configuration file')

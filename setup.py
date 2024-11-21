from setuptools import setup

setup(
    name='iotlab_mqtt_bridge',
    version='0.1.19',
    description='Bridge to connect IOT-LAB to a MQTT broker',
    long_description = open('README.md','r').read(),
    long_description_content_type = 'text/markdown',
    url='https://gitlab.irit.fr/rmess/locura/infra/iotlab_mqtt_bridge',
    author='Cassandre Vey',
    author_email='cassandre.vey@irit.fr',
    license='CeCILL 2.1',
    packages=['iotlab_mqtt_bridge'],
    install_requires=['iotlabcli>=3.3.0',
                      'paho-mqtt',
                      ],

    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
    ],
)


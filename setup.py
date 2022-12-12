from setuptools import setup

setup(
    name='locura_iotlab_bridge',
    version='0.1.2',    
    description='Bridge to connect LocURa MQTT ecosystem to IOT-LAB',
    url='https://gitlab.irit.fr/rmess/locura/infra/locura_iotlab_bridge',
    author='Quentin Vey',
    author_email='quentin.vey@irit.fr',
    license='CeCILL 2.1',
    packages=['locura_iotlab_bridge'],
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


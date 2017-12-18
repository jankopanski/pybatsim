with import <nixpkgs> {};
with pkgs.python36Packages;
with import (fetchTarball "https://gitlab.inria.fr/vreis/datamove-nix/repository/master/archive.tar.gz") {};

buildPythonPackage rec {
  name = "pybatsim";
  src = ./batsim;
  propagatedBuildInputs = with python36Packages; [
      sortedcontainers
      pyzmq
      redis
      pandas
      docopt
      # for testing
      coverage
      pytest
      # for doc generation
      sphinx
    ] ++ [ batsim ];

}

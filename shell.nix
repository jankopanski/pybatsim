with import <nixpkgs> {};
with pkgs.python36Packages;
with import (fetchTarball "https://gitlab.inria.fr/vreis/datamove-nix/repository/master/archive.tar.gz") {
  pkgs = (import <nixpkgs> {});
};

buildPythonPackage rec {
  name = "pybatsim-local";
  src = ./.;
  doCheck = false;
  propagatedBuildInputs = with python36Packages; [
      sortedcontainers
      pyzmq
      redis
      pandas
      docopt
      # for testing and debug
      coverage
      pytest
      ipython
      ipdb
      # for doc generation
      sphinx
    ];

}

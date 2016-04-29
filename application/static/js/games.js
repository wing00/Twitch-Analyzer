$(document).ready(function() {

	var games = new Bloodhound({
          datumTokenizer: Bloodhound.tokenizers.obj.whitespace('game'),
          queryTokenizer: Bloodhound.tokenizers.whitespace,
          identify: function(obj) {
              return obj.game;
          },
          prefetch: './static/games.json'
        });
        
        function gamesWithDefaults(q, sync) {
          if (q === '') {
            sync(games.get( "League of Legends", "Counter-Strike: Global Offensive", "Dota 2"));
          }
        
          else {
              games.search(q, sync);
          }
        }
        
        $('.typeahead').typeahead(
            {
                minLength: 0,
                highlight: true
            },
            {
                name: 'games',
                display: 'game',
                source: gamesWithDefaults
            }
        );
});  